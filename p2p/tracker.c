/*
 * tracker.c - Tracker server for the CS 4390 P2P file sharing project
 * Group 17: Bourinski, Calderon, Law, Shah   (tracker: Steven Law)
 *
 * Filled in from the instructor's skeleton (skeleton_tracker.rtf / tracker.c in project-501.zip).
 * We kept his structure and function names: main() accept loop -> peer_handler() ->
 * handle_list_req / handle_get_req / handle_createtracker_req / handle_updatetracker_req.
 *
 * One change from the skeleton: the skeleton uses fork() (a new process per peer), but the
 * project description says the tracker is multi-threaded and each peer is handed to a
 * worker thread, so we use pthread_create() instead.
 *
 * sconfig (one value per line):
 *   line 1: port number
 *   line 2: shared directory for tracker files (torrents)
 *   line 3: dead peer timeout in seconds (our assumption, default 900 = 15 min)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"

#define MAX_LINES 1000   // max lines we read from one tracker file

int server_port;            // from sconfig line 1
char shared_dir[256];       // from sconfig line 2
int dead_timeout = 900;     // from sconfig line 3, seconds

// only one thread at a time can read/write the .track files,
// so two peers updating the same file at once can't mess it up
pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
int worker_count = 0;       // numbers each worker thread in the log (multithreading proof)
pthread_mutex_t count_lock = PTHREAD_MUTEX_INITIALIZER;

void read_config();
void *peer_handler(void *arg);
void handle_list_req(int sock_child);
void handle_get_req(int sock_child, char *fname);
void handle_createtracker_req(int sock_child, char *read_msg);
void handle_updatetracker_req(int sock_child, char *read_msg);
void xtrct_fname(char *read_msg, char *fname);
int read_line(int sock, char *buf, int max);
void send_msg(int sock, char *msg, int len);
void clean_msg(char *msg);
int valid_name(char *name);
int get_md5(char *path, char *md5);


int main(){
    int sockid, sock_child;
    struct sockaddr_in server_addr, client_addr;
    socklen_t clilen = sizeof(client_addr);
    pthread_t tid;

    signal(SIGPIPE, SIG_IGN); // don't crash if a peer disconnects while we are replying
    setvbuf(stdout, NULL, _IONBF, 0); // print messages right away, even when output goes to a file
    read_config();

    if ((sockid = socket(AF_INET,SOCK_STREAM,0)) < 0){//create socket connection oriented
        printf("socket cannot be created \n"); exit(0);
    }
    int yes = 1;
    setsockopt(sockid, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)); // lets us restart on the same port right away

    //socket created at this stage
    //now associate the socket with local port to allow listening incoming connections
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;// assign address family
    server_addr.sin_port = htons(server_port);//change server port to NETWORK BYTE ORDER
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);// listen on all interfaces

    if (bind(sockid ,(struct sockaddr *) &server_addr,sizeof(server_addr)) ==-1){//bind and check error
        printf("bind failure\n"); exit(0);
    }

    printf("Tracker SERVER READY TO LISTEN INCOMING REQUEST.... (port %d)\n", server_port);
    if (listen(sockid, 10) < 0){ //listens at sockid and check error
        printf(" Tracker SERVER CANNOT LISTEN\n"); exit(0);
    }

    while(1) { //accept connection from every requester client
        if ((sock_child = accept(sockid ,(struct sockaddr *) &client_addr,&clilen))==-1){ /* accept connection and create a socket descriptor for actual work */
            printf("Tracker Cannot accept...\n");
            continue; // keep serving other peers instead of exiting
        }

        // skeleton used fork() here. We hand the peer to a new thread instead (spec: multi-threaded tracker).
        int *sock_ptr = malloc(sizeof(int));
        *sock_ptr = sock_child;
        if (pthread_create(&tid, NULL, peer_handler, sock_ptr) != 0){
            printf("Tracker cannot create thread\n");
            close(sock_child);
            free(sock_ptr);
            continue;
        }
        pthread_detach(tid); // thread cleans itself up when it is done

    } //accept loop ends
} // main fun ends


void *peer_handler(void *arg){ // each thread serves one peer request, then exits
    int sock_child = *(int *)arg;
    free(arg);
    char read_msg[MAXLINE];
    char fname[256];

    // log which worker thread serves which peer, so concurrent threads are visible
    pthread_mutex_lock(&count_lock);
    int worker_id = ++worker_count;
    pthread_mutex_unlock(&count_lock);
    struct sockaddr_in peer_addr;
    socklen_t peer_len = sizeof(peer_addr);
    char peer_ip[INET_ADDRSTRLEN] = "unknown";
    int peer_port = 0;
    if (getpeername(sock_child, (struct sockaddr *)&peer_addr, &peer_len) == 0){
        inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
        peer_port = ntohs(peer_addr.sin_port);
    }
    printf("Tracker: worker thread #%d started for peer %s:%d\n", worker_id, peer_ip, peer_port);

    //start handling client request
    int length = read_line(sock_child, read_msg, MAXLINE);
    if (length <= 0){ // peer closed without sending anything
        close(sock_child);
        printf("Tracker: worker thread #%d finished (peer sent nothing)\n", worker_id);
        return NULL;
    }
    clean_msg(read_msg); // remove the newline and the < > so both "<REQ LIST>" and "REQ LIST" work

    if((!strcmp(read_msg, "REQ LIST"))||(!strcmp(read_msg, "req list"))){//list command received
        handle_list_req(sock_child);// handle list request
        printf("Tracker: list request handled.\n");
    }
    // check createtracker/updatetracker before GET, so a filename containing "get" doesn't confuse it
    else if((!strncmp(read_msg,"createtracker",13))||(!strncmp(read_msg,"Createtracker",13))||(!strncmp(read_msg,"CREATETRACKER",13))){// createtracker command received
        handle_createtracker_req(sock_child, read_msg);
    }
    else if((!strncmp(read_msg,"updatetracker",13))||(!strncmp(read_msg,"Updatetracker",13))||(!strncmp(read_msg,"UPDATETRACKER",13))){// updatetracker command received
        handle_updatetracker_req(sock_child, read_msg);
    }
    else if((!strncmp(read_msg,"GET ",4))||(!strncmp(read_msg,"get ",4))){// get command received
        xtrct_fname(read_msg, fname);// extract filename from the command
        handle_get_req(sock_child, fname);
    }
    else {
        printf("Tracker: unknown command received\n"); // like the skeleton: no reply, just close
    }

    close(sock_child); // one request, one reply, then close (spec)
    printf("Tracker: worker thread #%d finished\n", worker_id);
    return NULL;
}//end client handler function


// createtracker filename filesize description md5 ip-address port-number
void handle_createtracker_req(int sock_child, char *read_msg){
    char fname[256], desc[256], md5[64], ip[64], path[600];
    long long filesize;
    int port;

    // %*s skips the word "createtracker"
    if (sscanf(read_msg, "%*s %255s %lld %255s %63s %63s %d", fname, &filesize, desc, md5, ip, &port) != 6
        || !valid_name(fname)){
        printf("Tracker: bad createtracker message\n");
        send_msg(sock_child, REP_CREATE_FAIL, strlen(REP_CREATE_FAIL));
        return;
    }
    sprintf(path, "%s/%s.track", shared_dir, fname);

    pthread_mutex_lock(&file_lock);
    FILE *fp = fopen(path, "r");
    if (fp != NULL){ // tracker file already exists
        fclose(fp);
        pthread_mutex_unlock(&file_lock);
        printf("Tracker: createtracker %s from %s:%d -> ferr (already exists)\n", fname, ip, port);
        send_msg(sock_child, REP_CREATE_FERR, strlen(REP_CREATE_FERR));
        return;
    }
    fp = fopen(path, "w");
    if (fp == NULL){
        pthread_mutex_unlock(&file_lock);
        printf("Tracker: createtracker %s -> fail (cannot create file)\n", fname);
        send_msg(sock_child, REP_CREATE_FAIL, strlen(REP_CREATE_FAIL));
        return;
    }
    // tracker file format from the project description; the seed holds the whole file
    fprintf(fp, "Filename: %s\n", fname);
    fprintf(fp, "Filesize: %lld\n", filesize);
    fprintf(fp, "Description: %s\n", desc);
    fprintf(fp, "MD5: %s\n", md5);
    fprintf(fp, "#list of peers follows next\n");
    fprintf(fp, "%s:%d:0:%lld:%ld\n", ip, port, filesize, (long)time(NULL)); // timestamp from tracker clock
    fclose(fp);
    pthread_mutex_unlock(&file_lock);

    printf("Tracker: createtracker %s from %s:%d -> succ\n", fname, ip, port);
    send_msg(sock_child, REP_CREATE_SUCC, strlen(REP_CREATE_SUCC));
}


// updatetracker filename start_bytes end_bytes ip-address port-number
void handle_updatetracker_req(int sock_child, char *read_msg){
    char fname[256], ip[64], path[600], reply[400];
    char *lines[MAX_LINES];
    char buf[300];
    long long start, end;
    int port, n = 0, found = 0;
    long now = (long)time(NULL);

    if (sscanf(read_msg, "%*s %255s %lld %lld %63s %d", fname, &start, &end, ip, &port) != 5
        || start > end || !valid_name(fname)){
        printf("Tracker: bad updatetracker message\n");
        sprintf(reply, REP_UPDATE_FAIL_FMT, "?");
        send_msg(sock_child, reply, strlen(reply));
        return;
    }
    sprintf(path, "%s/%s.track", shared_dir, fname);

    pthread_mutex_lock(&file_lock);
    FILE *fp = fopen(path, "r");
    if (fp == NULL){ // no tracker file for this filename
        pthread_mutex_unlock(&file_lock);
        printf("Tracker: updatetracker %s from %s:%d -> ferr (no tracker file)\n", fname, ip, port);
        sprintf(reply, REP_UPDATE_FERR_FMT, fname);
        send_msg(sock_child, reply, strlen(reply));
        return;
    }
    while (n < MAX_LINES && fgets(buf, sizeof(buf), fp) != NULL){ // read the whole file into memory
        lines[n] = strdup(buf);
        n++;
    }
    fclose(fp);

    fp = fopen(path, "w"); // rewrite the file with the updated peer list
    if (fp == NULL){
        for (int i = 0; i < n; i++) free(lines[i]);
        pthread_mutex_unlock(&file_lock);
        sprintf(reply, REP_UPDATE_FAIL_FMT, fname);
        send_msg(sock_child, reply, strlen(reply));
        return;
    }
    for (int i = 0; i < n; i++){
        char p_ip[64];
        int p_port;
        long long p_start, p_end;
        long p_time;
        // peer line format: ip:port:start:end:timestamp
        if (lines[i][0] != '#' && sscanf(lines[i], "%63[^:]:%d:%lld:%lld:%ld", p_ip, &p_port, &p_start, &p_end, &p_time) == 5){
            if (!strcmp(p_ip, ip) && p_port == port){ // this peer is already listed: update range + timestamp
                fprintf(fp, "%s:%d:%lld:%lld:%ld\n", ip, port, start, end, now);
                found = 1;
            }
            else if (now - p_time > dead_timeout){ // dead peer: its update interval passed, drop it
                printf("Tracker: removed dead peer %s:%d from %s\n", p_ip, p_port, fname);
            }
            else {
                fputs(lines[i], fp); // keep other live peers
            }
        }
        else {
            fputs(lines[i], fp); // header or comment line, keep it
        }
        free(lines[i]);
    }
    if (!found){ // new peer: add an entry with the current timestamp
        fprintf(fp, "%s:%d:%lld:%lld:%ld\n", ip, port, start, end, now);
    }
    fclose(fp);
    pthread_mutex_unlock(&file_lock);

    printf("Tracker: updatetracker %s from %s:%d bytes %lld-%lld -> succ\n", fname, ip, port, start, end);
    sprintf(reply, REP_UPDATE_SUCC_FMT, fname);
    send_msg(sock_child, reply, strlen(reply));
}


// reads Filename, Filesize and MD5 out of a .track file. returns 1 if it found all three
int read_track_header(char *path, char *name, long long *size, char *md5){
    char buf[300];
    int got = 0;
    FILE *fp = fopen(path, "r");
    if (fp == NULL) return 0;
    while (fgets(buf, sizeof(buf), fp) != NULL){
        if (sscanf(buf, "Filename: %255s", name) == 1) got++;
        else if (sscanf(buf, "Filesize: %lld", size) == 1) got++;
        else if (sscanf(buf, "MD5: %63s", md5) == 1) got++;
    }
    fclose(fp);
    return got == 3;
}


// returns 1 if the name ends in ".track"
int is_track_file(char *name){
    int len = strlen(name);
    return len > 6 && !strcmp(name + len - 6, ".track");
}


// REQ LIST: <REP LIST X>, then <n name size md5> for each tracker file, then <REP LIST END>
void handle_list_req(int sock_child){
    DIR *dir;
    struct dirent *entry;
    char path[600], name[256], md5[64], msg[600];
    long long size;
    int count = 0, i = 0;

    pthread_mutex_lock(&file_lock);
    dir = opendir(shared_dir);
    if (dir != NULL){ // first pass: count the tracker files
        while ((entry = readdir(dir)) != NULL){
            sprintf(path, "%s/%s", shared_dir, entry->d_name);
            if (is_track_file(entry->d_name) && read_track_header(path, name, &size, md5)) count++;
        }
        rewinddir(dir);
    }

    sprintf(msg, REP_LIST_BEGIN_FMT, count);
    send_msg(sock_child, msg, strlen(msg));

    if (dir != NULL){ // second pass: send one line per tracker file
        while ((entry = readdir(dir)) != NULL && i < count){
            sprintf(path, "%s/%s", shared_dir, entry->d_name);
            if (is_track_file(entry->d_name) && read_track_header(path, name, &size, md5)){
                i++;
                sprintf(msg, REP_LIST_ITEM_FMT, i, name, size, md5);
                send_msg(sock_child, msg, strlen(msg));
            }
        }
        closedir(dir);
    }
    pthread_mutex_unlock(&file_lock);

    send_msg(sock_child, REP_LIST_END, strlen(REP_LIST_END));
}


// GET filename.track: <REP GET BEGIN>, the file, <REP GET END md5-of-the-file>
void handle_get_req(int sock_child, char *fname){
    char path[600], md5[64], msg[200];

    if (is_track_file(fname)) sprintf(path, "%s/%s", shared_dir, fname);
    else sprintf(path, "%s/%s.track", shared_dir, fname); // also accept "GET movie1.avi"

    pthread_mutex_lock(&file_lock);
    FILE *fp = NULL;
    if (valid_name(fname)) fp = fopen(path, "rb");
    if (fp == NULL){
        pthread_mutex_unlock(&file_lock);
        // spec has no "not found" reply for GET; we send an empty reply (our assumption)
        printf("Tracker: GET %s -> no such tracker file\n", fname);
        sprintf(msg, REP_GET_END_FMT, "none");
        send_msg(sock_child, REP_GET_BEGIN, strlen(REP_GET_BEGIN));
        send_msg(sock_child, msg, strlen(msg));
        return;
    }
    fseek(fp, 0, SEEK_END); // find the file size
    long size = ftell(fp);
    rewind(fp);
    char *content = malloc(size + 1);
    long got = fread(content, 1, size, fp);
    fclose(fp);
    // md5 of the tracker file, so the peer can check it arrived correctly
    if (!get_md5(path, md5)) strcpy(md5, "none");
    pthread_mutex_unlock(&file_lock);

    sprintf(msg, REP_GET_END_FMT, md5);
    send_msg(sock_child, REP_GET_BEGIN, strlen(REP_GET_BEGIN));
    send_msg(sock_child, content, got);
    send_msg(sock_child, msg, strlen(msg));
    free(content);
    printf("Tracker: GET %s -> sent\n", fname);
}


// gets the md5 of a file by running the system's md5 command:
// md5sum on Linux, and if that isn't there, md5 -q on Mac
int get_md5(char *path, char *md5){
    char cmd[1300];
    sprintf(cmd, "md5sum '%s' 2>/dev/null || md5 -q '%s' 2>/dev/null", path, path);
    FILE *p = popen(cmd, "r");
    if (p == NULL) return 0;
    int ok = (fscanf(p, "%32s", md5) == 1);
    pclose(p);
    return ok;
}


// only allow simple filenames: letters, numbers, . _ and -
// (keeps peers from writing outside our folder or sneaking weird characters into commands)
int valid_name(char *name){
    if (name[0] == '\0' || name[0] == '.') return 0;
    for (int i = 0; name[i] != '\0'; i++){
        char c = name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
              || c == '.' || c == '_' || c == '-')) return 0;
    }
    return 1;
}


// pulls the filename out of "GET filename.track"
void xtrct_fname(char *read_msg, char *fname){
    fname[0] = '\0';
    sscanf(read_msg, "%*s %255s", fname);
}


// reads one line (up to '\n') from the socket. TCP is a byte stream, so we read until the newline.
int read_line(int sock, char *buf, int max){
    int i = 0;
    char c;
    while (i < max - 1){
        if (read(sock, &c, 1) <= 0) break; // closed or error
        if (c == '\n') break;
        buf[i] = c;
        i++;
    }
    buf[i] = '\0';
    return i;
}


// sends all len bytes (one write() might not send everything)
void send_msg(int sock, char *msg, int len){
    int sent = 0;
    while (sent < len){
        int n = write(sock, msg + sent, len - sent);
        if (n <= 0) return; // peer disconnected
        sent += n;
    }
}


// removes the trailing newline/spaces and the < > around a command
void clean_msg(char *msg){
    int len = strlen(msg);
    while (len > 0 && (msg[len-1] == '\n' || msg[len-1] == '\r' || msg[len-1] == ' ')){
        msg[len-1] = '\0';
        len--;
    }
    if (len > 0 && msg[len-1] == '>'){
        msg[len-1] = '\0';
        len--;
    }
    if (msg[0] == '<'){
        memmove(msg, msg + 1, len); // shift everything left one spot
    }
}


// reads port, shared directory and dead peer timeout from sconfig
void read_config(){
    FILE *fp = fopen("sconfig", "r");
    if (fp == NULL){
        printf("Tracker: cannot open sconfig\n");
        exit(1);
    }
    if (fscanf(fp, "%d %255s", &server_port, shared_dir) != 2){
        printf("Tracker: sconfig needs a port and a shared directory\n");
        exit(1);
    }
    if (fscanf(fp, "%d", &dead_timeout) != 1){ // optional third line
        dead_timeout = 900; // default: 15 minutes, same as the spec's default update interval
    }
    fclose(fp);

    mkdir(shared_dir, 0755); // create torrents/ if it isn't there (does nothing if it exists)
}

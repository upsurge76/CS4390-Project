/*
 * peer.c - Peer program for the CS 4390 P2P file sharing project
 * Group 17: Bourinski, Calderon, Law, Shah
 *
 * Filled in from the instructor's skeleton (skeleton_peer.c / peer.c in project-501.zip).
 * We kept his tracker connection code (socket -> connect -> write -> read -> close)
 * and his messages ("socket cannot be created", "Cannot connect to server").
 *
 * What this version does (midterm: whole file between two machines):
 *   - Server thread: listens on our port and sends pieces of files we have to other peers.
 *     Max 1024 bytes per request; bigger requests get <GET invalid> (spec).
 *   - Client side: a prompt where you type the four commands by hand (spec: manual testing).
 *   - On start-up, sends createtracker for every complete file in our shared folder (we are its seed).
 *   - Every update interval, sends updatetracker for every file we share.
 *   - After a GET, checks the tracker file's MD5, saves it to cache/, and starts the download
 *     automatically from the peer with the newest timestamp, 1024 bytes at a time.
 *
 * Still to do for the final: download from several peers at once (threads), resume after a crash,
 * and serving a file while it is still downloading.
 *
 * clientThreadConfig.cfg: line 1 tracker port, line 2 tracker IP, line 3 update interval (seconds)
 * serverThreadConfig.cfg: line 1 port this peer listens on, line 2 shared folder name
 *
 * Peer-to-peer request (spec leaves the format to us):  <GET filename start_byte end_byte>\n
 *   start is included, end is not (bytes start .. end-1), end - start must be 1024 or less.
 *   Reply: exactly end - start bytes of the file, or <GET invalid>\n
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"

#define SEGMENT_SIZE 262144   // download is split into 256 KB segments (spec: segment size is our choice)
#define MAX_REPLY    65536    // biggest tracker reply we read

int tracker_port;          // clientThreadConfig.cfg line 1
char tracker_ip[64];       // clientThreadConfig.cfg line 2
int update_interval;       // clientThreadConfig.cfg line 3 (seconds)
int my_port;               // serverThreadConfig.cfg line 1
char shared_dir[256];      // serverThreadConfig.cfg line 2
char my_ip[64];            // found automatically (spec: not stored in a config file)
char peer_name[64];        // "Peer1" from the folder name peer1, printed on every message

void read_config();
void find_my_ip();
int connect_to(char *ip, int port);
int talk_to_tracker(char *msg, char *reply, int max);
void *server_thread(void *arg);
void *serve_peer(void *arg);
void *update_thread(void *arg);
void send_createtrackers();
void send_updates();
void handle_get_reply(char *fname, char *reply);
void download_file(char *track_path);
int read_line(int sock, char *buf, int max);
int read_bytes(int sock, char *buf, int n);
void send_msg(int sock, char *msg, int len);
void clean_msg(char *msg);
int get_md5(char *path, char *md5);
int valid_name(char *name);
long long file_size(char *path);


int main(int argc, char *argv[]){
    pthread_t tid;
    char line[MAXLINE], cmd[MAXLINE], msg[MAXLINE + 4];
    char *reply = malloc(MAX_REPLY);

    signal(SIGPIPE, SIG_IGN); // don't crash if the other side disconnects
    setvbuf(stdout, NULL, _IONBF, 0); // print messages right away, even when output goes to a file
    read_config();
    find_my_ip();
    mkdir(shared_dir, 0755); // make sure our folders exist
    mkdir("cache", 0755);    // local cache for tracker files we GET
    printf("%s: started at %s:%d, sharing folder %s/\n", peer_name, my_ip, my_port, shared_dir);

    pthread_create(&tid, NULL, server_thread, NULL); // serve other peers while we use the prompt
    pthread_detach(tid);
    sleep(1); // give the server thread a moment to start listening

    send_createtrackers(); // we are the seed for every complete file we already have

    pthread_create(&tid, NULL, update_thread, NULL); // periodic updatetracker
    pthread_detach(tid);

    printf("%s: type REQ LIST, GET <file>.track, createtracker ..., updatetracker ..., or quit\n", peer_name);
    while (1){
        printf("%s> ", peer_name);
        fflush(stdout);
        if (fgets(line, sizeof(line), stdin) == NULL) break; // end of input
        strcpy(cmd, line);
        clean_msg(cmd); // so both "<REQ LIST>" and "REQ LIST" work
        if (cmd[0] == '\0') continue;
        if (!strcmp(cmd, "quit") || !strcmp(cmd, "exit")) break;

        if (!strcmp(cmd, "REQ LIST") || !strcmp(cmd, "req list")
            || !strncmp(cmd, "createtracker", 13) || !strncmp(cmd, "updatetracker", 13)){
            printf("%s: %s\n", peer_name, cmd);
            sprintf(msg, "<%s>\n", cmd);
            if (talk_to_tracker(msg, reply, MAX_REPLY) > 0) printf("%s", reply);
        }
        else if (!strncmp(cmd, "GET ", 4) || !strncmp(cmd, "get ", 4)){
            char fname[256];
            sscanf(cmd, "%*s %255s", fname);
            printf("%s: Get %s\n", peer_name, fname);
            sprintf(msg, "<GET %s>\n", fname);
            if (talk_to_tracker(msg, reply, MAX_REPLY) > 0) handle_get_reply(fname, reply);
        }
        else {
            printf("%s: unknown command\n", peer_name);
        }
    }
    printf("%s terminated\n", peer_name);
    exit(0);
}


// the skeleton's connection code, turned into a function we can reuse.
// returns a connected socket, or -1 if it couldn't connect
int connect_to(char *ip, int port){
    struct sockaddr_in server_addr;
    int sockid;

    if ((sockid = socket(AF_INET,SOCK_STREAM,0))==-1){//create socket
        printf("%s: socket cannot be created\n", peer_name);
        return -1;
    }
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;//host byte order
    server_addr.sin_port = htons(port);// convert to network byte order
    server_addr.sin_addr.s_addr = inet_addr(ip);
    if (connect(sockid ,(struct sockaddr *) &server_addr,sizeof(struct sockaddr))==-1){//connect and error check
        close(sockid);
        return -1;
    }
    return sockid;
}


// sends one message to the tracker and reads the whole reply (tracker closes after replying).
// returns the reply length, or -1 if the tracker can't be reached
int talk_to_tracker(char *msg, char *reply, int max){
    int sockid = connect_to(tracker_ip, tracker_port);
    if (sockid < 0){
        printf("%s: Cannot connect to server\n", peer_name);
        reply[0] = '\0';
        return -1;
    }
    send_msg(sockid, msg, strlen(msg));
    int total = 0, n;
    while (total < max - 1 && (n = read(sockid, reply + total, max - 1 - total)) > 0){
        total += n;
    }
    reply[total] = '\0';
    close(sockid);
    return total;
}


// finds our own IP by connecting to the tracker and asking the socket which address it used
void find_my_ip(){
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int sockid = connect_to(tracker_ip, tracker_port);
    if (sockid < 0){
        printf("%s: Cannot connect to server at %s:%d\n", peer_name, tracker_ip, tracker_port);
        exit(1);
    }
    getsockname(sockid, (struct sockaddr *)&addr, &len);
    strcpy(my_ip, inet_ntoa(addr.sin_addr));
    close(sockid); // tracker sees an empty request and just closes it
}


// ---------------- server side: give file pieces to other peers ----------------

void *server_thread(void *arg){
    int sockid, sock_child;
    struct sockaddr_in server_addr, client_addr;
    socklen_t clilen = sizeof(client_addr);
    pthread_t tid;

    if ((sockid = socket(AF_INET,SOCK_STREAM,0)) < 0){
        printf("%s: socket cannot be created\n", peer_name); exit(1);
    }
    int yes = 1;
    setsockopt(sockid, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(my_port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sockid, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1){
        printf("%s: bind failure on port %d\n", peer_name, my_port); exit(1);
    }
    if (listen(sockid, 10) < 0){
        printf("%s: cannot listen\n", peer_name); exit(1);
    }
    while (1){ // one thread per peer that connects to us
        if ((sock_child = accept(sockid, (struct sockaddr *)&client_addr, &clilen)) == -1) continue;
        int *sock_ptr = malloc(sizeof(int));
        *sock_ptr = sock_child;
        if (pthread_create(&tid, NULL, serve_peer, sock_ptr) != 0){
            close(sock_child);
            free(sock_ptr);
            continue;
        }
        pthread_detach(tid);
    }
    return NULL;
}


// answers <GET filename start end> requests from one peer until it disconnects
void *serve_peer(void *arg){
    int sock_child = *(int *)arg;
    free(arg);
    char req[MAXLINE], fname[256], path[600], buf[MAX_CHUNK];
    long long start, end;

    while (read_line(sock_child, req, MAXLINE) > 0){
        clean_msg(req);
        if (sscanf(req, "GET %255s %lld %lld", fname, &start, &end) != 3
            || !valid_name(fname) || start < 0 || end <= start
            || end - start > MAX_CHUNK){ // spec: more than 1024 bytes is an error, not a smaller send
            send_msg(sock_child, REP_GET_INVALID, strlen(REP_GET_INVALID));
            break;
        }
        sprintf(path, "%s/%s", shared_dir, fname);
        FILE *fp = fopen(path, "rb");
        if (fp == NULL || end > file_size(path)){ // we don't have those bytes
            if (fp != NULL) fclose(fp);
            send_msg(sock_child, REP_GET_INVALID, strlen(REP_GET_INVALID));
            break;
        }
        fseek(fp, start, SEEK_SET);
        int n = fread(buf, 1, end - start, fp);
        fclose(fp);
        send_msg(sock_child, buf, n);
    }
    close(sock_child);
    return NULL;
}


// ---------------- talking to the tracker automatically ----------------

// returns 1 for files we should share: normal files that aren't half-downloaded (.part)
int is_shareable(char *name){
    int len = strlen(name);
    if (name[0] == '.') return 0;
    if (len > 5 && !strcmp(name + len - 5, ".part")) return 0;
    return valid_name(name);
}


// on start-up: createtracker for every complete file in our shared folder
void send_createtrackers(){
    DIR *dir = opendir(shared_dir);
    struct dirent *entry;
    char path[600], md5[64], msg[1000], reply[200];
    if (dir == NULL) return;
    while ((entry = readdir(dir)) != NULL){
        if (!is_shareable(entry->d_name)) continue;
        sprintf(path, "%s/%s", shared_dir, entry->d_name);
        long long size = file_size(path);
        if (size <= 0 || !get_md5(path, md5)) continue;
        // description can't have spaces, so we use underscores
        sprintf(msg, "<createtracker %s %lld Shared_by_%s %s %s %d>\n", entry->d_name, size, peer_name, md5, my_ip, my_port);
        printf("%s: createtracker %s %lld Shared_by_%s %s %s %d\n", peer_name, entry->d_name, size, peer_name, md5, my_ip, my_port);
        if (talk_to_tracker(msg, reply, sizeof(reply)) > 0) printf("%s: %s", peer_name, reply);
    }
    closedir(dir);
}


// updatetracker for every file we share (we have all of it: bytes 0 to size)
void send_updates(){
    DIR *dir = opendir(shared_dir);
    struct dirent *entry;
    char path[600], msg[1000], reply[200];
    if (dir == NULL) return;
    while ((entry = readdir(dir)) != NULL){
        if (!is_shareable(entry->d_name)) continue;
        sprintf(path, "%s/%s", shared_dir, entry->d_name);
        long long size = file_size(path);
        if (size <= 0) continue;
        sprintf(msg, "<updatetracker %s 0 %lld %s %d>\n", entry->d_name, size, my_ip, my_port);
        talk_to_tracker(msg, reply, sizeof(reply));
    }
    closedir(dir);
}


void *update_thread(void *arg){
    while (1){
        sleep(update_interval);
        send_updates();
    }
    return NULL;
}


// ---------------- downloading ----------------

// after GET: check the MD5, save the tracker file to cache/, then start the download
void handle_get_reply(char *fname, char *reply){
    char *begin = strstr(reply, REP_GET_BEGIN);
    char *end = strstr(reply, "<REP GET END ");
    char md5_sent[64], md5_got[64], track_path[600];

    if (begin == NULL || end == NULL || sscanf(end, "<REP GET END %63[^>]>", md5_sent) != 1
        || !strcmp(md5_sent, "none")){
        printf("%s: tracker has no tracker file %s\n", peer_name, fname);
        return;
    }
    begin += strlen(REP_GET_BEGIN);
    if (!valid_name(fname)) return;
    if (strstr(fname, ".track") == NULL) sprintf(track_path, "cache/%s.track", fname);
    else sprintf(track_path, "cache/%s", fname);

    FILE *fp = fopen(track_path, "wb"); // save what we got, then check its MD5
    if (fp == NULL) return;
    fwrite(begin, 1, end - begin, fp);
    fclose(fp);
    if (!get_md5(track_path, md5_got) || strcmp(md5_got, md5_sent) != 0){
        printf("%s: tracker file MD5 does not match, discarding it\n", peer_name);
        remove(track_path);
        return;
    }
    printf("%s: got tracker file %s (MD5 ok), starting download\n", peer_name, fname);
    download_file(track_path); // spec: download starts automatically after GET
}


#define MAX_PEERS 100   // most peers we read from one tracker file

// downloads the whole file from the peer with the newest timestamp, 1024 bytes per request
void download_file(char *track_path){
    char line[300], fname[256] = "", md5[64] = "", ip[64], best_ip[64] = "";
    char part_path[600], final_path[600], md5_got[64], msg[700], reply[200];
    char peer_ip[MAX_PEERS][64];   // peers that have the whole file
    int peer_port[MAX_PEERS];
    long peer_ts[MAX_PEERS];
    long long filesize = -1, s, e;
    long ts;
    int port, best_port = 0, npeers = 0;

    FILE *fp = fopen(track_path, "r");
    if (fp == NULL) return;
    while (fgets(line, sizeof(line), fp) != NULL){
        if (line[0] == '#') continue; // comment lines are ignored (spec)
        if (sscanf(line, "Filename: %255s", fname) == 1) continue;
        if (sscanf(line, "Filesize: %lld", &filesize) == 1) continue;
        if (sscanf(line, "MD5: %63s", md5) == 1) continue;
        if (sscanf(line, "%63[^:]:%d:%lld:%lld:%ld", ip, &port, &s, &e, &ts) == 5 && npeers < MAX_PEERS){
            int is_me = (!strcmp(ip, my_ip) && port == my_port);
            if (!is_me && s == 0 && e >= filesize){ // for now we need a peer with the whole file
                strcpy(peer_ip[npeers], ip);
                peer_port[npeers] = port;
                peer_ts[npeers] = ts;
                npeers++;
            }
        }
    }
    fclose(fp);

    if (fname[0] == '\0' || filesize < 0 || !valid_name(fname)){
        printf("%s: tracker file is missing information\n", peer_name);
        return;
    }
    sprintf(final_path, "%s/%s", shared_dir, fname);
    if (file_size(final_path) == filesize){
        printf("%s: already have %s\n", peer_name, fname);
        remove(track_path); // we don't need the tracker file anymore
        return;
    }
    // try the peer with the newest timestamp first (spec). tracker info can be old, so if
    // that peer is gone, try the next newest one instead of giving up (spec)
    int sockid = -1;
    while (sockid < 0){
        int best = -1;
        for (int i = 0; i < npeers; i++){
            if (peer_ts[i] >= 0 && (best < 0 || peer_ts[i] > peer_ts[best])) best = i;
        }
        if (best < 0) break; // tried everyone
        sockid = connect_to(peer_ip[best], peer_port[best]);
        if (sockid < 0){
            printf("%s: cannot connect to %s %d, trying another peer\n", peer_name, peer_ip[best], peer_port[best]);
        }
        else {
            strcpy(best_ip, peer_ip[best]);
            best_port = peer_port[best];
        }
        peer_ts[best] = -1; // mark this peer as tried
    }
    if (sockid < 0){
        printf("%s: no reachable peer has all of %s yet\n", peer_name, fname);
        return;
    }
    sprintf(part_path, "%s/%s.part", shared_dir, fname); // .part until it is complete
    FILE *out = fopen(part_path, "wb");
    if (out == NULL){ close(sockid); return; }

    char buf[MAX_CHUNK];
    long long got = 0;
    int ok = 1;
    while (got < filesize && ok){
        long long seg_end = got + SEGMENT_SIZE; // segments are taken in order (spec)
        if (seg_end > filesize) seg_end = filesize;
        printf("%s: downloading %lld to %lld bytes of %s from %s %d\n", peer_name, got, seg_end, fname, best_ip, best_port);
        while (got < seg_end){ // one segment = several requests of at most 1024 bytes
            int n = (seg_end - got > MAX_CHUNK) ? MAX_CHUNK : (int)(seg_end - got);
            sprintf(msg, "<GET %s %lld %lld>\n", fname, got, got + n);
            send_msg(sockid, msg, strlen(msg));
            if (read_bytes(sockid, buf, n) != n){ ok = 0; break; } // peer left or said <GET invalid>
            fwrite(buf, 1, n, out);
            got += n;
        }
        if (ok){ // only after a whole segment: tell the tracker what we have (spec)
            fflush(out);
            sprintf(msg, "<updatetracker %s 0 %lld %s %d>\n", fname, got, my_ip, my_port);
            talk_to_tracker(msg, reply, sizeof(reply));
        }
    }
    fclose(out);
    close(sockid);

    if (!ok){
        printf("%s: download of %s stopped at %lld bytes (peer stopped sending)\n", peer_name, fname, got);
        return;
    }
    if (!get_md5(part_path, md5_got) || strcmp(md5_got, md5) != 0){ // check against the tracker file's MD5
        printf("%s: %s MD5 does not match, download failed\n", peer_name, fname);
        return;
    }
    rename(part_path, final_path);
    remove(track_path); // spec: delete the tracker file from the cache once the file is complete
    printf("%s: %s download complete (MD5 verified)\n", peer_name, fname);
}


// ---------------- small helpers (same as in tracker.c) ----------------

// reads one line (up to '\n') from the socket
int read_line(int sock, char *buf, int max){
    int i = 0;
    char c;
    while (i < max - 1){
        if (read(sock, &c, 1) <= 0) break;
        if (c == '\n') break;
        buf[i] = c;
        i++;
    }
    buf[i] = '\0';
    return i;
}


// reads exactly n bytes (TCP may give them to us in pieces). returns how many we got
int read_bytes(int sock, char *buf, int n){
    int got = 0;
    while (got < n){
        int r = read(sock, buf + got, n - got);
        if (r <= 0) break;
        got += r;
    }
    return got;
}


// sends all len bytes
void send_msg(int sock, char *msg, int len){
    int sent = 0;
    while (sent < len){
        int n = write(sock, msg + sent, len - sent);
        if (n <= 0) return;
        sent += n;
    }
}


// removes the trailing newline/spaces and the < > around a message
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
        memmove(msg, msg + 1, len);
    }
}


// md5 of a file using the system command: md5sum on Linux, md5 -q on Mac
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
int valid_name(char *name){
    if (name[0] == '\0' || name[0] == '.') return 0;
    for (int i = 0; name[i] != '\0'; i++){
        char c = name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
              || c == '.' || c == '_' || c == '-')) return 0;
    }
    return 1;
}


// size of a file in bytes, or -1 if it doesn't exist
long long file_size(char *path){
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    return st.st_size;
}


// reads both config files and works out our name from the folder we run in (peer1 -> Peer1)
void read_config(){
    char cwd[512];
    if (getcwd(cwd, sizeof(cwd)) != NULL){
        char *base = strrchr(cwd, '/');
        strcpy(peer_name, base ? base + 1 : cwd);
        if (peer_name[0] >= 'a' && peer_name[0] <= 'z') peer_name[0] -= 32; // capital first letter
    }
    else strcpy(peer_name, "Peer");

    FILE *fp = fopen("clientThreadConfig.cfg", "r");
    if (fp == NULL || fscanf(fp, "%d %63s %d", &tracker_port, tracker_ip, &update_interval) != 3){
        printf("%s: clientThreadConfig.cfg needs tracker port, tracker IP and update interval\n", peer_name);
        exit(1);
    }
    fclose(fp);

    fp = fopen("serverThreadConfig.cfg", "r");
    if (fp == NULL || fscanf(fp, "%d %255s", &my_port, shared_dir) != 2){
        printf("%s: serverThreadConfig.cfg needs a port and a shared folder name\n", peer_name);
        exit(1);
    }
    fclose(fp);
}

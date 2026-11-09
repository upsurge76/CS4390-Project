
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>



main(int argc,char *argv[]){
	
   	char server_address[50];
	int server_port=3490;  // you should instead read from configuration file
    
   
	struct sockaddr_in server_addr;
	int sockid;
        
	if ((sockid = socket(AF_INET,SOCK_STREAM,0))==-1){//create socket
		printf("socket cannot be created\n"); exit(0);
	}
                                              
   
    server_addr.sin_family = AF_INET;//host byte order
    server_addr.sin_port = htons(server_port);// convert to network byte order
    if (connect(sockid ,(struct sockaddr *) &server_addr,sizeof(struct sockaddr))==-1){//connect and error check
		printf("Cannot connect to server\n"); exit(0);
	}

   /* If connected successfully*/
    
	if(!strcmp(argv[1],"list")){// if this is LIST command
		int list_req=htons(LIST);
		if((write(sockid,(char *)&list_req,sizeof(list_req))) < 0){//inform the server of the list request
			printf("Send_request  failure\n"); exit(0);
		}

        if((read(sockid,(char *)&msg,sizeof(msg)))< 0){// read what server has said
			printf("Read  failure\n"); exit(0); 
		}
		
		close(sockid);
		printf("Connection closed\n");
	    exit(0);
	}//end close

    
}	     
  

/*
 * protocol.h - all the message strings for the CS 4390 P2P project (Group 17)
 *
 * Tracker and peers both include this file so the messages always match.
 * Formats are copied exactly from the project description. Every message ends in \n.
 *
 * Peer -> tracker:
 *   <createtracker filename filesize description md5 ip-address port-number>\n
 *   <updatetracker filename start_bytes end_bytes ip-address port-number>\n
 *   <REQ LIST>\n
 *   <GET filename.track>\n
 */
#ifndef PROTOCOL_H
#define PROTOCOL_H

#define MAXLINE   4096   // longest message line we read
#define MAX_CHUNK 1024   // max bytes a peer can ask another peer for in one GET

// createtracker replies
#define REP_CREATE_SUCC "<createtracker succ>\n"
#define REP_CREATE_FERR "<createtracker ferr>\n"   // tracker file already exists
#define REP_CREATE_FAIL "<createtracker fail>\n"   // any other error

// updatetracker replies (%s = filename)
#define REP_UPDATE_SUCC_FMT "<updatetracker %s succ>\n"
#define REP_UPDATE_FERR_FMT "<updatetracker %s ferr>\n" // no such tracker file
#define REP_UPDATE_FAIL_FMT "<updatetracker %s fail>\n" // unable to update

// LIST reply
#define REP_LIST_BEGIN_FMT "<REP LIST %d>\n"      // %d = number of files
#define REP_LIST_ITEM_FMT  "<%d %s %lld %s>\n"    // number, filename, filesize, md5
#define REP_LIST_END       "<REP LIST END>\n"

// GET (tracker file) reply
#define REP_GET_BEGIN   "<REP GET BEGIN>\n"
#define REP_GET_END_FMT "<REP GET END %s>\n"      // %s = md5 of the tracker file we sent

// peer server thread reply when asked for more than MAX_CHUNK bytes
#define REP_GET_INVALID "<GET invalid>\n"

#endif

#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include<fcntl.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;
using namespace std;
//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}


StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    //
    /*
     1. create Context and response message pointer.
     2. set teh deadline
     3. obtain the client writer.
     4. open the file
     5. send the file size.
     6. successively use the writer to send the values out.
     7. once done, observe the status of the return value without concerning about the response.
     */
    ::grpc::ClientContext context;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    
    //create response msg
    dfs_service::Msg response;
    dfs_service::Data fileData;
    
    //create clinet writer.
    std::unique_ptr< ::grpc::ClientWriter< ::dfs_service::Data>> writer = service_stub->storeFile(&context,&response);
    
    //open the file
    string fullPath = mount_path + filename;
    int fdes = open(fullPath.c_str(),O_RDONLY,0664);
        //handline file not found.
    if (fdes < 0)
    {
        return StatusCode::NOT_FOUND;
    }
    struct stat fileStat;
    //send the filename to the server
    fileData.set_dataval(filename);
    writer->Write(fileData);
    //get filesize and send to server
    fstat(fdes, &fileStat);
    size_t fileSize = fileStat.st_size;
    fileData.set_dataval(to_string(fileSize));
    writer->Write(fileData);
    
    //start sending file in chunks
    size_t leftBytes = fileSize;
    size_t sentTotal = 0;
    size_t thisChunk = BUFF_SIZE;
    size_t sentNow = 0;
    char buffer[BUFF_SIZE];
    memset(buffer, 0, BUFF_SIZE);
    
    while(leftBytes > 0)
    {
        thisChunk = BUFF_SIZE;
        if (leftBytes < BUFF_SIZE)
        {
            thisChunk = leftBytes;
        }
        sentNow = pread(fdes, buffer, thisChunk, sentTotal);
        fileData.set_dataval(buffer, sentNow);
        writer->Write(fileData);
        //update counters
        leftBytes -= sentNow;
        sentTotal += sentNow;
        memset(buffer, 0, BUFF_SIZE);
    }
    
    Status serverStatus = writer->Finish();
    if(serverStatus.error_code() == StatusCode::OK)
    {
        cout<<"OK"<<endl;
        return StatusCode::OK;
    }
    else if(serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        cout<<"Deadline exceeded"<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    return StatusCode::CANCELLED;
}

StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    cout<<"client fetch "<<filename<<endl;
    /*
     1. use the stub to call the server and get the clientReader.
     2. Check if the writer is still present to see if the file is found. if not, return.
     3. create a file and start receiving the message and storing in the file.
     */
    //create reader
    unique_ptr<::grpc::ClientReader<::dfs_service::Data>> reader;
    //create context and return request.
    ::grpc::ClientContext clientContext;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    clientContext.set_deadline(deadline);
    
    //message to request file
    ::dfs_service::Msg request;
    
    //fill in the file name into the request
    request.set_msgval(filename);
    
    //send request to server by calling the stub and get the reader.
    reader = service_stub->fetchFile(&clientContext,request);
    
    //use the reader to read into a local data variable
    ::dfs_service::Data fileData;
    
    // Read the file size. if -1, didn't find.
    reader->Read(&fileData);
    
    long long int fileSize = stoi(fileData.dataval());
    if (fileSize < 0)
    {
        cout<<"file not found"<<endl;
        return StatusCode::NOT_FOUND;
    }
    //receive the data into a buffer and write it into a file.
        //create a complete file path with the mount
    string wholeName = mount_path+filename;
    char buffer[BUFF_SIZE];
    int fdes = open(wholeName.c_str(),O_WRONLY|O_CREAT,0644);
    
    //transfer to file
    size_t bytesLeft = fileSize;
    size_t bytesTransferred = 0;
    size_t chunkSize = BUFF_SIZE;
    
    while(reader->Read(&fileData))
          {
              //set chunk size
              chunkSize = BUFF_SIZE;
              if (bytesLeft < BUFF_SIZE)
              {
                  chunkSize = bytesLeft;
              }
              memset(buffer, 0, BUFF_SIZE);
              memcpy(buffer, fileData.dataval().c_str(), BUFF_SIZE);
              
              //write it into the file
              size_t thisTransferBytes = pwrite(fdes, buffer, chunkSize, bytesTransferred);
              
              //update counters
              bytesTransferred += thisTransferBytes;
              bytesLeft -= thisTransferBytes;
    }
    
    //close the reader.
    close(fdes);
    Status serverStatus =  reader->Finish();
    if (serverStatus.error_code() == StatusCode::OK)
    {
        //its all ok.
        return StatusCode::OK;
    }
    else if(serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        if(DEBUG) cout<<"Deadline exceeded"<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
cout<<"client delete "<<filename<<endl;
    /*
     send the file to be deleted.
     1. create stub which returns the status. look at the status. thats. it.
     
     */
    //create context.
    ::grpc::ClientContext clientContext;
    //create deadline
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    clientContext.set_deadline(deadline);
    
    //Create request
    dfs_service::Msg request;
    request.set_msgval(filename);
    
    //response
    dfs_service::Msg response;
    
    //call method
    Status serverStatus = service_stub->deleteFile(&clientContext,request,&response);
    
    //OK, NOTFOund, deadline and other
    if (serverStatus.error_code() == StatusCode::OK)
    {
        if(DEBUG) cout<<"status ok"<<endl;
        return StatusCode::OK;
    }
    else if (serverStatus.error_code() == StatusCode::NOT_FOUND)
    {
        if(DEBUG) cout<<"status not found"<<endl;
        return StatusCode::NOT_FOUND;
    }
    else if (serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        if(DEBUG) cout<<"status deadline exceeded"<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    
    return StatusCode::ABORTED;
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
    /*
     aim: to obtain a map of file name and mtime and display it if the bool variable is true.
     1. create context and set deadline
     2. obtain hte client reader usgin the stub.
     3. whilethe server sends each element of the list entry, add them to the provided map structure
     4. if it doesn't reach the end, call the deadline exceeded.
     
     */
    ClientContext context;
    dfs_service::Msg clientMsg; // not important.
    clientMsg.set_msgval("Requesting List");
    
    //create deadline
     std::chrono::system_clock::time_point deadline =
     std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
     context.set_deadline(deadline);
    
    //reader pointer
    std::unique_ptr<grpc::ClientReader<dfs_service::ListInfo>> reader = service_stub->listFiles(&context,clientMsg);
    //create a ListInfo unit
    dfs_service::ListInfo elementLI;
    
    //start reading
//    if(DEBUG)cout<<"start reading"<<endl;
    while(reader->Read(&elementLI))
    {
        //create a map element
//        if(DEBUG)cout<<"create a map element"<<endl;
        file_map->insert(pair<string, int>(elementLI.filenameval(),elementLI.mtime()));
    }
    
    Status serverStatus = reader->Finish();

    //print the list
    if (true)
    {
        map<string,int>::iterator listItr = file_map->begin();
        while(listItr != file_map->end())
        {
            cout<<"Filename: "<<listItr->first<<" mtime: "<<listItr->second<<endl;
            listItr++;
        }
    }
    
    if(serverStatus.error_code() == StatusCode::OK)
    {
        if (DEBUG) cout<<"status OK"<<endl;
        return StatusCode::OK;
    }
    else if(serverStatus.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        if (DEBUG) cout<<"Deadline exceeded"<<endl;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    
    return StatusCode::CANCELLED;
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    cout<<"stat for "<<filename<<endl;
    return StatusCode::OK;
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//


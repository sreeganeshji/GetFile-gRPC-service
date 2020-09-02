#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include<fcntl.h>
#include<unistd.h>


#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;

using namespace std;

//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:
    
    //file table
    unordered_map<string, FileElement> fileTable;

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
        
        //load all files in mount_dir
        if (DEBUG) cout<<"loading files from mount dir"<<endl;
        
        struct dirent *fileInfo;
        struct stat fileStatus;
        int fdes;
        DIR *folder;
        if((folder = opendir(mount_path.c_str())) != NULL)
        {
            while((fileInfo = readdir(folder)) != NULL)
            {
                if(fileInfo->d_type == DT_REG)
                {
                    //found a file. add to fileTable.
                    char buffer[100];
                    memset(buffer,0,100);
                    strcpy(buffer, mount_path.c_str());
                    strcat(buffer, fileInfo->d_name);
                    fdes = open(buffer,O_RDWR,0644);
                    if(fdes<0)
                    {
                        cout<<"Couldn't open files"<<endl;
                        cout<<strerror(errno)<<endl;
                        closedir(folder);
                        return;
                    }
                    fstat(fdes, &fileStatus);
                    //add to map.
                    if(DEBUG) cout<<"loading "<<fileInfo->d_name<<" size: "<<fileStatus.st_size<<endl;
                    fileTable[fileInfo->d_name] = FileElement(fdes, fileStatus);
                }
            }
            closedir(folder);
        }
        
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

  /*
   Need to structure the saved file into a data structure whcih will hold information whcih would be needed to access it later and will have information whcih woudl bbe relavant to the clinet like the filename, the modified time. maybe the creation time. are these part of the stat of the file? must be.
   the fstat should have the required information.
   things needed to represent the file.
   1. file descriptor.
   2. fstat
   during hte list operation, the server needs to send the list of the file name and hte mtime, but the mtime can be found out during the time of sending. hence not needing to be computed.
   */
    
    
    Status storeFile(::grpc::ServerContext* context, ::grpc::ServerReader< ::dfs_service::Data>* reader, ::dfs_service::Msg* response) override{
        /*
         1. use the reader to read the file name and file size.
         2. create a file of that name.
         3. successively read and store the data into the file.
         4. in parallel, check that the deadline is not exceeded.
         
         */
        //reading fileName and Size
        string fileName;
        size_t fileSize;
        dfs_service::Data fileData;
        reader->Read(&fileData);
        fileName = fileData.dataval();
        reader->Read(&fileData);
        fileSize = stoi(fileData.dataval());
        
        //create a file of that name
        string fullPath = mount_path + fileName;
        int fdes = open(fullPath.c_str(),O_CREAT|O_WRONLY, 0664);
        struct stat fileStat;
        
        //write into the file
        char buffer[BUFF_SIZE];
        size_t totalWrite = 0;
        size_t writeLeft = fileSize;
        size_t thisChunk = BUFF_SIZE;
        size_t nowWrite = 0;
        memset(buffer, 0, BUFF_SIZE);
        while(writeLeft > 0)
        {
            //check for deadline
            if(context->IsCancelled())
            {
                cout<<"Deadline Exceeded"<<endl;
                return Status(StatusCode::DEADLINE_EXCEEDED,"Deadline exceeded");
            }
            thisChunk = BUFF_SIZE;
            if (writeLeft < BUFF_SIZE)
            {
                thisChunk = writeLeft;
            }
            reader->Read(&fileData);
            memcpy(buffer, fileData.dataval().c_str(), thisChunk);
            nowWrite = pwrite(fdes, buffer, thisChunk, totalWrite);
            
            //update the counters
            totalWrite += nowWrite;
            writeLeft -= nowWrite;
        }
        //add entry to fileTable
        fstat(fdes, &fileStat);
        fileTable[fileName] = FileElement(fdes, fileStat);
        
        return Status::OK;
        
    }
    
    ::grpc::Status fetchFile(::grpc::ServerContext* context, const ::dfs_service::Msg* request, ::grpc::ServerWriter< ::dfs_service::Data>* writer) override
    {
        /*
         the message will have the filename.
         1. get the fd
         2. open the file.
         3. read it into a buffer in chuncks and send it successively through the writer.
         4. Finish() the writer
         5. handle FILE_NOT_FOUND, DEADLINE_EXCEEDED and other faults.
         */
        ::dfs_service::Data fileData;
        
        unordered_map<string, FileElement>::iterator fileItr = fileTable.find(request->msgval());
        if (fileItr == fileTable.end())
        {//send -1 for the size
            fileData.set_dataval(to_string(-1));
            writer->Write(fileData);
            
            cout<<"no file found"<<endl;
            return Status(StatusCode(StatusCode::NOT_FOUND),"file not found");
        }
        FileElement thisFile = fileItr->second;

//        create buffer
        char buffer[BUFF_SIZE];
//        find file size
        size_t fileSize = thisFile.fileStatus.st_size;
        
//        send this to the client
        fileData.set_dataval(to_string(fileSize));
        writer->Write(fileData);
        
//        use the writer to write these chunks
        size_t totalWritten = 0;
        size_t leftBytes = fileSize;
        size_t thisChunk = BUFF_SIZE;
        while(totalWritten < fileSize)
        {
            //check for deadline
            if(context->IsCancelled())
               {
                   cout<<"timeout error"<<endl;
                   return(Status(StatusCode::DEADLINE_EXCEEDED,"timeout"));
               }
            
            //set the chunk size;
            thisChunk = BUFF_SIZE;
            
            if (leftBytes < BUFF_SIZE)
            {
                thisChunk = leftBytes;
            }
            memset(buffer, 0, BUFF_SIZE);
            size_t thisWriteBytes = pread(thisFile.fileDescriptor, buffer, thisChunk, totalWritten);
            fileData.set_dataval(buffer, thisChunk);
            writer->Write(fileData);
            
            //update counters
            totalWritten += thisWriteBytes;
            leftBytes -= thisWriteBytes;
        }
        
        return Status::OK;
    }
    
    //delete file
    Status deleteFile(::grpc::ServerContext* context, const ::dfs_service::Msg* request, ::dfs_service::Msg* response) override
    {
        /*
         1. use the request to identify the file in the table.
         1.1 if not, send not found
         2. add the mount path to the file name adn remove it.
         3. remove its entry in the file table.
         4. send ok
         */
        unordered_map<string, FileElement>::iterator fileItr;
        //find the requested file
        if((fileItr = fileTable.find(request->msgval())) == fileTable.end())
        {
            //file not found.
            if (DEBUG) cout<<"File not found"<<endl;
            return Status(StatusCode::NOT_FOUND, "FILE NOT FOUND");
        }
        //create the whole path
        string wholePath = mount_path + request->msgval();
        
        //delete the file
        remove(wholePath.c_str());
        
        //delete entry from table.
        fileTable.erase(request->msgval());
        
        return Status::OK;
    }
    
    ::grpc::Status listFiles(::grpc::ServerContext* context, const ::dfs_service::Msg* request, ::grpc::ServerWriter< ::dfs_service::ListInfo>* writer) override
    {
        /*
         if there is no entry on the file map list, return. else send line by line. we can fine tune the behavior later during submission.
         1. Don't need to use the received request message.
         2. use the server writer to write each entry of the unordered_map
         3. send finish.
         */
        if(DEBUG) cout<<"list files"<<endl;
        //iterator to traverse the elements in the array
        unordered_map<string, FileElement>::iterator itr = fileTable.begin();
        
        while(itr != fileTable.end())
        {
            //check for deadline
           if(context->IsCancelled())
              {
                  cout<<"timeout error"<<endl;
                  return(Status(StatusCode::DEADLINE_EXCEEDED,"timeout"));
              }
            //create a ListInfo element and write it usign the writer
            cout<<"create an element"<<endl;
            dfs_service::ListInfo elementLI;
            elementLI.set_filenameval(itr->first);
            int fileMtime = (int)(((itr->second).fileStatus.st_mtim).tv_sec);
            cout<<"setting mtime"<<fileMtime<<endl;
            elementLI.set_mtime(fileMtime);
            //write into the channel
            cout<<"write into teh channel"<<endl;
            writer->Write(elementLI);
            //increment itr
            itr++;
        }
        return Status::OK;
        
    }

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//


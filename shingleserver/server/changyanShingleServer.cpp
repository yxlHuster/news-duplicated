#include "shingleService.h"
#include "shingle.h"
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>
#include <config.h>
#include <protocol/TCompactProtocol.h>
#include <protocol/TBinaryProtocol.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <pthread.h>
#include <string>
#include <vector>
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace std;

class shingleServiceHandler : virtual public shingleServiceIf {
public:
    shingleServiceHandler() {
    }

    void getShingleString(std::vector<std::string> & _return, const std::string& contents) {
        _return.clear();      
        if (contents.size() < 1) {
              return;
        }
        std::vector<std::string> results = yahooShinglesForStrings((unsigned char*)contents.c_str(), 6, 84);
        for (int i = 0; i < results.size(); i++) {
            _return.push_back(results[i]);
        }  
    }

    void getShingleLong(std::vector<int64_t> & _return, const std::string& contents) {
        _return.clear();
        if (contents.size() < 1) {
            return;
        }
        std::vector<fprint_t> results = yahooShinglesForLongs((unsigned char*)contents.c_str(), 6, 84);
        for (int i = 0; i < results.size(); i++) {
            _return.push_back(results[i]);
        }
   } 
   void getSimDocuments(std::vector<int64_t> & _return, const std::string& contents) {
    //TODO: This need to build cache in server
    _return.clear();
   }
};

int main(int argc, char **argv) {
    int port = 9911;
    shared_ptr<shingleServiceHandler> handler(new shingleServiceHandler());
    shared_ptr<TProcessor> processor(new shingleServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TCompactProtocolFactory());
    int multinum = 500;
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(multinum);
    shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    printf("start shingle server...%d thread\n", multinum);
    TThreadPoolServer server(processor, serverTransport, transportFactory, protocolFactory, threadManager);
    server.setTimeout(20);
    server.serve();
    return 0;
}


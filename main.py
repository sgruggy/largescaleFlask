## GRPC For recieving message and search query can be put in main file

## can all put our parts in seperate files and  import to main

## GRPC will also need to be added into scalica to send the message and eventually search query to us

from concurrent import futures
import time

import grpc

import search_pb2
import search_pb2_grpc

import pymongo
import mongo_stuff

class TextSearch(search_pb2_grpc.TextSearchServicer):
    def getMessage(self, request, context):

        print(request)
        newMsg = mongo_stuff.insert_message(request)
        print("Message insert success")
        mongo_stuff.serialize_message_to_word(newMsg)    
        
        return search_pb2.MessageReply(code='OK')

    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    search_pb2_grpc.add_TextSearchServicer_to_server(TextSearch(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        print("grpc server running")
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("grpc server has stopped")
        server.stop(0)

if __name__ == '__main__':
    serve()
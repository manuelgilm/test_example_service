import grpc 
import service
from service.service_spec import reading_files_pb2_grpc
from service.service_spec import reading_files_pb2

from sending_email import send_email
import multiprocessing
import concurrent.futures as futures
import sys
import logging

logging.basicConfig(level=10, format="%(asctime)s - [%(levelname)8s] - %(name)s - %(message)s")
log = logging.getLogger("filereader_service")

# Create a class to be added to the gRPC server
# derived from the protobuf codes.
class MessageServicer(reading_files_pb2_grpc.MessageServicer):
    def __init__(self):
        log.debug("Reader Servicer Created! ...")

    def printing(self, request, context):
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(target = send_email, args=(
            request.model_path,
            "experimentalmlalgorithms@gmail.com",
            "manuelgilsitio@gmail.com",
            "manuelsalvadorgilmatheus666"
        ))
        p.start()
        p.join()

        response = return_dict.get("response", None)
        if not response or "error" in response:
            error_msg = response.get("error", None) if response else None
            log.error(error_msg)
            context.set_details(error_msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return reading_files_pb2.Output()

        log.debug(f"Message Sended = OK | Model Path: {request.model_path}")
        return reading_files_pb2.Output()

# The gRPC serve function.
#
# Params:
# max_workers: pool of threads to execute calls asynchronously
# port: gRPC server port
#
# Add all your classes to the server here.
def serve(max_workers=10, port=7777):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    reading_files_pb2_grpc.add_MessageServicer_to_server(MessageServicer(), server)
    server.add_insecure_port("[::]:{}".format(port))
    return server

if __name__ == "__main__":
    """
    Runs the gRPC server to communicate with the Snet Daemon.
    """
    parser = service.common.common_parser(__file__)
    args = parser.parse_args(sys.argv[1:])
    service.common.main_loop(serve, args)
# gRPC-file-service

TO IMPLEMENT GRPC PROCEDURE CALLS TO PERFORM FILE OPERATIONS LIKE DELETE, FETCH, STORE, ETC.
# Design
The different procedures to delete, fetch, store, etc was implemented using gRPC libraries. the template of the procedures and the messages involved were declared as a protocol buffer which is compiled to generate the methods which implements the procedures to call the procedures using the socket interface. The client takes the requests and accordingly initiates the transfer of data in either as a stream or as blocks.
# Flow of operation
The client creates a context and sets the deadline parameter. it loads the request and the expected response structures onto a stub and calls the procedure. It waits for the response from the server. The server receives the context with the request. It performs the operation of fetch, store, etc, using the file libraries. It then sets the response and sends the status of the operation.
# Implementation and testing
The files in the mount folder are loaded at the start. The operations of store and fetch require data streams from client to the server and vice versa. A buffer mechanism is implemented to read files onto and transfer using the grpc interface. The transfer involves keeping track of the current sent data and sending the next remaining data offset by the currently sent bytes. This ensures to handle dynamic transfer sizes.
The grpc implementation was tested for the different operations of delete, fetch, store, etc., of different sized files. also for accurate data transfer, deadline protection.
# References
The implementation details of the grpc protocols were referred to from the grpc tutorials and examples on the site https://grpc.io

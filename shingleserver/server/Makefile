INCLUDET=/usr/local/include/thrift/
INCLUDE_GENERATE_HEADER=../gen-cpp/
LIBPATHO=/usr/local/lib/ 
LIBPATHT=/usr/lib64/
CC=g++
CC_FLAG=-DHAVE_NETINET_IN_H
RPG=shingleServer
SHINGLE_PATH=./
DEST_EXE_PATH=../bin/
OBJ=shingleService.o  shingles_constants.o  shingles_types.o  mi_ctype8.o html.o fprint.o  shingle.o changyanShingleServer.o

$(RPG):$(OBJ)
	$(CC) -L $(LIBPATHO) -L $(LIBPATHT) -lthrift  -o $(RPG) $(OBJ)
	cp $(RPG) ${DEST_EXE_PATH}
shingleService.o: 
	$(CC) -c -g ${CC_FLAG} -I ${INCLUDET} -I ${INCLUDE_GENERATE_HEADER}  ${INCLUDE_GENERATE_HEADER}/shingleService.cpp
shingles_constants.o: 
	$(CC) -c -g ${CC_FLAG} -I ${INCLUDET} -I ${INCLUDE_GENERATE_HEADER}  ${INCLUDE_GENERATE_HEADER}/shingles_constants.cpp
shingles_types.o: 
	$(CC) -c -g ${CC_FLAG} -I ${INCLUDET} -I ${INCLUDE_GENERATE_HEADER}  ${INCLUDE_GENERATE_HEADER}/shingles_types.cpp
changyanShingleServer.o: 
	$(CC) -c -g ${CC_FLAG} -I ${INCLUDE_GENERATE_HEADER} -I ${INCLUDET} changyanShingleServer.cpp
mi_ctype8.o: 
	$(CC) -c -g   ${SHINGLE_PATH}/mi_ctype8.cpp
html.o: 
	$(CC) -c -g   ${SHINGLE_PATH}/html.cpp
fprint.o: 
	$(CC) -c -g   ${SHINGLE_PATH}/fprint.cpp
shingle.o: 
	$(CC) -c -g   ${SHINGLE_PATH}/shingle.cpp
.PRONY:clean
clean:
	@echo "Removing linked and compiled files......"
	rm -f $(RPG) ${OBJ_PATH}$(OBJ)


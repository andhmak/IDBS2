error_secondary_main: error_secondary_main.o hash_file.o sht_file.o
	@echo " Compile error_secondary_main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/error_secondary_main.o ./build/hash_file.o ./build/sht_file.o -lbf -o ./bin/runner

secondary_main_big: secondary_main_big.o hash_file.o sht_file.o
	@echo " Compile secondary_main_big ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/secondary_main_big.o ./build/hash_file.o ./build/sht_file.o -lbf -o ./bin/runner

secondary_main: secondary_main.o hash_file.o sht_file.o
	@echo " Compile secondary_main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/secondary_main.o ./build/hash_file.o ./build/sht_file.o -lbf -o ./bin/runner

error_main: error_main.o hash_file.o
	@echo " Compile error_main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/error_main.o ./build/hash_file.o -lbf -o ./bin/runner

main: main.o hash_file.o
	@echo " Compile main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/main.o ./build/hash_file.o -lbf -o ./bin/runner

main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/main.o ./src/main.c -O2

error_main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/error_main.o ./src/error_main.c -O2

secondary_main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/secondary_main.o ./src/secondary_main.c -O2

secondary_main_big.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/secondary_main_big.o ./src/secondary_main_big.c -O2

error_secondary_main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/error_secondary_main.o ./src/error_secondary_main.c -O2

hash_file.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/hash_file.o ./src/hash_file.c -O2

sht_file.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/sht_file.o ./src/sht_file.c -O2

ht: 
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./bin/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./bin/runner -O2

run: 
	./bin/runner

clean:
	@echo " Delete binary, build and data ...";
	-rm -f ./bin/runner ./build/main.o ./build/error_main.o ./build/hash_file.o ./data.db ./data1.db ./data_1.db ./data_2.db ./data_3.db ./data_4.db ./data_5.db ./data_6.db ./data_7.db ./data_8.db ./data_9.db ./data_10.db ./data_11.db ./data_12.db
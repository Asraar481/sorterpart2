#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
//#include <sys/stat.h>
//#include <sys/wait.h>
#include <sys/types.h>
#include <ctype.h>
#include <pthread.h>
#include "sorter.h"
#define DEBUG 1
#define ERRORS 1

/*proj2 notes
put -pthread compilation
only 1 csv w/ everything sorted
need to output all tids
*Note:define debug and errors to 0
*Note: sorted files?
*/

//checks if a give filename is a .csv file or a directory
//returns 0 if directory, 1 if .csv, and -1 if neither
typedef struct categories{
	struct categories* nextCat;
	char* name;
	int index;
	char dataType;
}Category;

void trim(char* c){ //trims whitespace
	char* start;
	char* end;
    for(start = c; *start; start++){ //Find first character that is not whitespace
    	if (!isspace(start[0])){
    		break;
    	}
    }
    for(end = start + strlen(start); end > start + 1; end--){//Find start of last whitespace
    	if (!isspace(end[-1])){
    		break;
    	}
    }
    *end = 0;
    if(start > c){//move characters from "start" to the beginning of the string
    	memmove(c, start, (end - start) + 1);
}
}

int freeCatRecur(Category* front){ //free Category Linked List
	if(front == NULL){
		return 1;
	}
	Category* ptr = front->nextCat;
	free(front->name);
	free(front);
	if(ptr != NULL){
		return freeCatRecur(ptr);
	}
	return 0;
}

int freeRow(Row* head, int numColumns){ //free Row linked List
	if(head == NULL)
		return 1;
	//print line to STDOUT
	free(head->line);
	int i;
	for(i = 0; i < numColumns; i++){
		free(head->info[i]);
	}
	Row* ptr = head->nextRow;
	free(head);
	return freeRow(ptr, numColumns);
	return 0;
}

//char **-from 0 to len of **, int columns
int printCat(Category* front){ //recursively prints categories for testing purposes
	if(front == NULL){
		return 1;
	}
	Category* ptr = front->nextCat;
	printf("%s\n",front->name);
	if(ptr != NULL){
		return printCat(ptr);
	}
	return 0;
}

int checkCSVorDirectory(char* fileName, char* currentDir){
	char* string = strrchr(fileName, '.');
	if(string == NULL){
		struct stat s;
		char* path = (char*)malloc(strlen(fileName) + strlen(currentDir) + 2);
		strcpy(path, currentDir);
		strcat(path, "/");
		strcat(path, fileName);
		//is a directory
		if (stat(path, &s) == 0 && S_ISDIR(s.st_mode)){
			return 0;
		}
		free(path);
		//not a directory or a .csv
		return -1;
	}
	else if(strcmp(string, ".csv") == 0){
		return 1;
	}
	else{
		return -1;
	}
}

char *createPath(char* fileName, char* directoryPath){
	if(fileName == NULL || directoryPath == NULL){
		return NULL;
	}
	char* path = (char*)malloc(strlen(fileName) + strlen(directoryPath) + 2); //+2 for '/' and '\0'
	strcpy(path, directoryPath);
	strcat(path, "/");
	strcat(path, fileName);
	return path;
}

//main vars
char* col = NULL;//which column to sort on
int* colNum = NULL;//guess of index of column to sort on
Row* finalFirstRow = NULL;
pthread_mutex_t mainLock;//lock to access final first row
char* threads = NULL;//list of threads - strcat, both null term, check sizeof/strlen, realloc
int threadcount = 0;//*Note: do we include the main
pthread_mutex_t threadLock;//lock for accessing threads and threadcount
typedef struct tidLL{
	struct tidLL* next;
	pthread_t tid;
}threadnode;
//Category* firstCat; 
//Row* firstRow = NULL;
char* columns; //getline() for first row of csv
//data types for the 28 movie columns
char dataArr[28] = {'s', 's', 'i', 'i', 'i', 'i', 's', 'i', 'i', 's',
's', 's', 'i', 'i', 's', 'i', 's', 's', 'i', 's',
's', 's', 'i', 'i', 'i', 'd', 'd', 'i'}; 
//should I use this to compare or accept if there are spaces?
char* firstLine = "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n";


//funct ptrs

/*
takes in ptr: char* of path of csv
returns first row
*/
void* csvSort(void* ptr){
	//open file, input cat, rows
	//change to struct cat
	//sort rows, mergesort
	//change to csvpath instead of file ptr

	FILE *origin = fopen((char*) ptr, "r");
	if(origin == NULL){
		if(ERRORS){
			printf("\ncannot find file -csvSort \n");
		}
		pthread_exit(NULL);
		return NULL;
	}

	int numColumns;
	Category* firstCat; 
	Row* firstRow = NULL;
	char* columns;
	char* buffer = NULL; size_t length = 0; int charRead = 0; //initialize variables for getline()
	charRead = getline(&buffer, &length, origin);//get line with categories
	if(!(strcmp(buffer, firstLine)) || charRead == -1){//if there is nothing in the file
		if(ERRORS){
			printf("\ngetline failed for csvSort \n");
		}
		pthread_exit(NULL);
		return NULL;
	}
	columns = (char*)malloc(sizeof(char)*(charRead+1));
	strcpy(columns, buffer);
	buffer = strsep(&buffer, "\n"); // remove new line character for every row
	char* token;
	char* running = strdup(buffer);
	int count = 0; //count index postion
	if(running[0] == '"'){//check if first char is '"'
		strsep(&running, "\"");
	token = strsep(&running, "\"");
	strsep(&running, ",");
	}else{
		token = strsep(&running, ",");
	}
	trim(token);
	firstCat = (Category*)malloc(sizeof(Category));
	firstCat->name = (char*)malloc(sizeof(char)*(strlen(token)+1));
	strcpy(firstCat->name, token);

	firstCat->index = count;
	firstCat->dataType = dataArr[count];
	Category *ptr = firstCat;
	while(token != NULL && running != NULL){//check if theres no more tokens left
		ptr->nextCat = (Category*)malloc(sizeof(Category));
		if(running[0] == '"'){
			strsep(&running, "\"");
			token = strsep(&running, "\"");
			strsep(&running, ",");
		}else{
			token = strsep(&running, ",");
		}
		trim(token);
		count++;
		ptr->nextCat->name = (char*)malloc(sizeof(char)*(strlen(token)+1));
		strcpy(ptr->nextCat->name, token);
		
		ptr->nextCat->index = count;
		ptr->nextCat->dataType = dataArr[count];
		ptr = ptr->nextCat;
	}
	numColumns = count + 1;

	//read in rows


	charRead = getline(&buffer, &length, origin); //read next line
	firstRow = (Row*)malloc(sizeof(Row));
	firstRow->line = (char*)malloc(sizeof(char)*(strlen(buffer)+1));
	firstRow->info = (char**)malloc(sizeof(char*) * numColumns);
	strcpy(firstRow->line, buffer);
	buffer = strsep(&buffer, "\n");
	char* run = strdup(buffer);
	int numRow = 1;
	int currIndex = 0;
	for(currIndex; currIndex<numColumns && run != NULL; currIndex++){
		if(run[0] == '"'){//check if first char is '"'
			strsep(&run, "\"");
			token = strsep(&run, "\"");
			strsep(&run, ",");
		}else{
			token = strsep(&run, ",");
		}
		trim(token);
		firstRow->info[currIndex] = (char*)malloc(sizeof(char)*(strlen(token)+1));
		strcpy(firstRow->info[currIndex], token);
	}

	Row* rowPtr = firstRow; //pointer to first row
	charRead = getline(&buffer, &length, origin); //read second line
	while(charRead != -1){ //parse through each line in the file 
		rowPtr->nextRow = (Row*)malloc(sizeof(Row));
		rowPtr->nextRow->line = (char*)malloc(sizeof(char)*(charRead+1));
		rowPtr->nextRow->info = (char**)malloc(sizeof(char*)*numColumns);
		strcpy(rowPtr->nextRow->line, buffer);
		buffer = strsep(&buffer, "\n");
		run = (char*)realloc(run, strlen(buffer)+1);
		strcpy(run, buffer);
		
		currIndex = 0;
		for(currIndex; currIndex<numColumns && run != NULL; currIndex++){
			if(run[0] == '"'){//check if first char is '"'
				strsep(&run, "\"");
			token = strsep(&run, "\"");
			strsep(&run, ",");
			}else{
				token = strsep(&run, ",");
			}
			trim(token);
			rowPtr->nextRow->info[currIndex] = (char*)malloc(sizeof(char)*(strlen(token)+1));
			strcpy(rowPtr->nextRow->info[currIndex], token);
		}
		rowPtr = rowPtr->nextRow;
		numRow++;
		charRead = getline(&buffer, &length, origin);
	}
	rowPtr->nextRow = NULL;




	//change cat to array, check index then traverse
	char type;
	int z; //index
	Category* traverse = firstCat; //traverse category list to get index postion of column to sort by and data type
	while(traverse != NULL){
		if(strcmp(traverse->name, col) == 0){
			type = traverse->dataType;
			z = traverse->index;
			break;
		}
		traverse = traverse->nextCat;
	}
	if(traverse == NULL){
		freeCatRecur(firstCat);
		freeRow(firstRow, numColumns);
		free(columns);
		if(ERRORS){
			printf("\nColumn not found. Please enter proper parameters.\n");//error, column not found
			fflush(stdout);
		}
		pthread_exit(NULL);
		return NULL;
	}

	
	firstRow = mergeSort(firstRow, numRow, z, type);
	
	fclose(origin);
	free(ptr);
	
	freeCatRecur(firstCat);
	//freeRow(firstRow, numColumns);//at end of main
	free(columns);
	//exit or return w/ first Row
	pthread_exit(firstRow);
	return firstRow;

}


/*
takes in ptr: char* startdirpath
Returns first row
*/
void* dirSort(void* ptr){

	//add new lock
	struct dirent *d; 
	DIR *startDir;
	char* startDirPath = (char *) ptr;
	startDir = opendir(startDirPath);
	if(startDir == NULL){
		if(DEBUG||ERRORS)
			printf("Start Directory Not Found\n");
		return NULL;
	}
	pthread_mutex_t dirLock = PTHREAD_MUTEX_INITIALIZER;;//lock for this dir --destroy when u return
	threadnode* threads = NULL;//tids of threads
	threadnode* lastThread = NULL;//last in LL
	threadnode* newThread = NULL;
	Row* firstRow = NULL;//master first row for this dir
	int create = 0;

	while ((d = readdir (startDir)) != NULL) {  
		if(strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0 && strstr(d->d_name, "-sorted-") == NULL){
			int x = checkCSVorDirectory(d->d_name, startDirPath);
			if(x == 0){ //d->d_name is a directory

				char* temp = createPath(d->d_name, startDirPath);
				startDirPath = temp;
				//pthread create dir sort
				newThread = (threadnode*)malloc(sizeof(threadnode));
				newThread->next = NULL;
				create = pthread_create(&(newThread->tid), NULL, dirSort, (void *)temp);
				if(!create){
					free(newThread);
				}
				//startDir = opendir(startDirPath);
			}
			else if(x == 1){//d->d_name is a csv file so sort it
				char* csvName = (char*)malloc(strlen(d->d_name)+1);
				strcpy(csvName, d->d_name);
				char *csvPath = createPath(csvName, startDirPath);
				/*(char*)malloc(strlen(csvName) + strlen(startDirPath) + 2);
				strcpy(csvPath, startDirPath);
				strcat(csvPath, "/");
				strcat(csvPath, csvName);
				*/
				FILE *origin = fopen(csvPath, "r");
				if(origin == NULL){
					return 1;
				}
				//free(csvPath);
				free(csvName);

				//pthread create
				newThread = (threadnode*)malloc(sizeof(threadnode));
				newThread->next = NULL;
				create = pthread_create(&(newThread->tid), NULL, csvSort, (void *)csvPath);
				if(!create){
					free(newThread);
				}
				
			}
			else{ //neither a csv file or directory
				continue;
			}

			if(threads == NULL){
				threads = newThread;
				lastThread = newThread;
			}else{
				lastThread->next = newThread;
				lastThread = newThread;
			}
		}
	}

	//while/for loop for join, merge into firstRow
	//lock


	threadnode* Threadptr = threads;
	threadnode* Threadptr2 = threads;
	char* newTID = NULL;

	pthread_mutex_lock(&threadLock);//crit sec: updating list of tids, tidcount
	while(Threadptr != NULL){
		Threadptr2 = Threadptr;

		threadcount++;//malloc enough, 1+2+,+\0 , strcopy first, then strcat
		//newTID = (char*) malloc(sizeof(int));
		asprintf(&newTID, "%d", Threadptr->tid);
		if(threads == NULL){
			threads = newTID;
			newTID = NULL;
		}else{
			threads = realloc(threads, strlen(threads) + strlen(newTID) + 3);
			strcat(threads, ",");
			strcat(threads, newTID);
			free(newTID);
		}
		Threadptr=Threadptr->next;
		free(Threadptr2);
	}
	pthread_mutex_unlock(&threadLock);

	pthread_mutex_destroy(&dirLock);
	pthread_exit(firstRow);
	return firstRow;
}

int main(int argc, char **argv){
	//*Note: intialize 2 global locks
	//3-7 arguments in command line input
	if(argc == 3 || argc == 5 || argc == 7){
		struct dirent *d; 
		DIR *startDir;
		DIR *outputDir;
		char* outputDirPath;
		char* startDirPath;

		if(argc == 3 && !(strcmp(argv[1], "-c"))){
			col = (char*)malloc(strlen(argv[2])+1);
			strcopy(col, argv[2]);
			startDir = opendir(".");
			startDirPath = ".";
			outputDir = NULL;
		}else  if(argc == 5){
			if(!(strcmp(argv[1], "-c"))){
				col = (char*)malloc(strlen(argv[2])+1);
				strcopy(col, argv[2]);

				if(!(strcmp(argv[3], "-o"))){
					outputDir = opendir(argv[4]);
					outputDirPath = argv[4];
					startDir = opendir(".");
					startDirPath = ".";
				}else if(!(strcmp(argv[3], "-d"))){
					startDir = opendir(argv[4]);
					startDirPath = argv[4];
					outputDir = NULL;
				}else{
					printf("Error in inputs\n");
					return -1;
				}
			}else if(!(strcmp(argv[3], "-c"))){
				col = (char*)malloc(strlen(argv[4])+1);
				strcopy(col, argv[4]);

				if(!(strcmp(argv[1], "-o"))){
					outputDir = opendir(argv[2]);
					outputDirPath = argv[2];
					startDir = opendir(".");
					startDirPath = ".";
				}else if(!(strcmp(argv[1], "-d"))){
					startDir = opendir(argv[2]);
					startDirPath = argv[2];
					outputDir = NULL;
				}else{
					printf("Error in inputs\n");
					return -1;
				}
			}else{
				printf("Error inputs\n");
				return -1;
			}
		}else if(argc == 7){
			if(!(strcmp(argv[1], "-c")) && !(strcmp(argv[3], "-d")) && !(strcmp(argv[5], "-o"))){
				col = (char*)malloc(strlen(argv[2])+1);
				strcopy(col, argv[2]);
				outputDir = opendir(argv[6]);
				outputDirPath = argv[6];
				startDir = opendir(argv[4]);
				startDirPath = argv[4];

			}else if(!(strcmp(argv[1], "-c")) && !(strcmp(argv[3], "-o")) && !(strcmp(argv[5], "-d"))){
				col = (char*)malloc(strlen(argv[2])+1);
				strcopy(col, argv[2]);
				outputDir = opendir(argv[4]);
				outputDirPath = argv[4];
				startDir = opendir(argv[6]);
				startDirPath = argv[6];

			}else if(!(strcmp(argv[1], "-d")) && !(strcmp(argv[3], "-c")) && !(strcmp(argv[5], "-o"))){
				col = (char*)malloc(strlen(argv[4])+1);
				strcopy(col, argv[4]);
				outputDir = opendir(argv[6]);
				outputDirPath = argv[6];
				startDir = opendir(argv[2]);
				startDirPath = argv[2];

			}else if(!(strcmp(argv[1], "-d")) && !(strcmp(argv[3], "-o")) && !(strcmp(argv[5], "-c"))){
				col = (char*)malloc(strlen(argv[6])+1);
				strcopy(col, argv[6]);
				outputDir = opendir(argv[4]);
				outputDirPath = argv[4];
				startDir = opendir(argv[2]);
				startDirPath = argv[2];

			}else if(!(strcmp(argv[1], "-o")) && !(strcmp(argv[3], "-c")) && !(strcmp(argv[5], "-d"))){
				col = (char*)malloc(strlen(argv[4])+1);
				strcopy(col, argv[4]);
				outputDir = opendir(argv[2]);
				outputDirPath = argv[2];	outputDirPath = argv[6];
				startDir = opendir(argv[4]);
				startDirPath
				startDir = opendir(argv[6]);
				startDirPath = argv[6];

			}else if(!(strcmp(argv[1], "-o")) && !(strcmp(argv[3], "-d")) && !(strcmp(argv[5], "-c"))){
				col = (char*)malloc(strlen(argv[6])+1);
				strcopy(col, argv[6]);
				outputDir = opendir(argv[2]);
				outputDirPath = argv[2];
				startDir = opendir(argv[4]);
				startDirPath = argv[4];

			}else{
				printf("Error in inputs\n");
				return -1;
			}
			//if outputDir doesn't exist final sorted csv file is inputted into where the search first began
			if(outputDir == NULL){
				outputDir = startDir;
				outputDirPath = startDirPath;
			}
		}else{
			printf("Error in inputs\n");
			return -1;
		}

		//you can start actually coding here

	}

	else{
		printf("Error in inputs\n");
		return -1;
	}


	/*
	if(argc > 2 && argc < 8){

		//check if correctly sorting by column
		if(!(strcmp(argv[1], "-c"))){
			//set up directory search
			col = (char*)malloc(strlen(argv[2])+1);
			strcopy(col, argv[2]);
			struct dirent *d; 
			DIR *startDir;
			DIR *outputDir;
			char* outputDirPath;
			char* startDirPath;

			//we are sorting outputting in another directory
			if(argc == 5 || argc == 7){
				
				//Check for whether we will be sorting by directory. If we're searching in the current
				//directory, then there is a chance that we will have the output directory as the third
				//input parameter
				

				if(!(strcmp(argv[3],"-d")) || !(strcmp(argv[3], "-o"))){
					if(!(strcmp(argv[3],"-d"))){
						startDir = opendir(argv[4]);
						startDirPath = argv[4];
						if(startDir == NULL){
							printf("Start Directory Not Found\n");
							return -1;
						}
						if(argc == 7){
							outputDir = opendir(argv[6]);
							outputDirPath = argv[6];
							if(outputDir == NULL){
								printf("Output Directory Error\n");
								return -1;
							}
						}
						else{
							outputDir = NULL;
						}
					}
					else{
						startDir = opendir(".");
						startDirPath = ".";
						outputDir = opendir(argv[4]);
						outputDirPath = argv[4];
						if(outputDir == NULL){
							printf("Output Directory Error\n");
							return -1;
						}
					}

				}

				//incorrect input parameters
				else{
					printf("Incorrect inputs\n");
					return -1;
				}



			}
			else if(argc == 3){
					startDir = opendir(".");
					startDirPath = ".";
					outputDir = NULL;
				}
				else{
					printf("Incorrect inputs\n");
					return -1;
				}


			//go through current directory and find csv files and directories
			pid_t pid, ipid = (int)getpid();
			printf("Initial PID: %d\n", ipid);
			printf("TIDS of all child processes: ");
			fflush(stdout);	
			//while loop
					while ((d = readdir (startDir)) != NULL) {  
						if(strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0 && strstr(d->d_name, "-sorted-") == NULL){
							int x = checkCSVorDirectory(d->d_name, startDirPath);
					if(x == 0){ //d->d_name is a directory
						pid = fork();
						if(pid < 0){
							printf("Fork Error\n");
							return 1;
						}
						else if(pid > 0){ //parent process continues going through current directory
							continue;
						}
						else{ //child process goes through next directory
							printf("%d,", (int)getpid());
							fflush(stdout);
							char* temp = createPath(d->d_name, startDirPath);
							/*(char*)malloc(strlen(d->d_name) + strlen(startDirPath) + 2);
							strcpy(temp, startDirPath);
							strcat(temp, "/");
							strcat(temp, d->d_name);
							*//*
							startDirPath = temp;
							//pthread create dirsort w/ startDirPath
							startDir = opendir(startDirPath);
						}
					}
					else if(x == 1){//d->d_name is a csv file so sort it
						pid = fork();
						if(pid < 0){
							printf("Fork error\n");
							return 1;
						}
						else if(pid > 0){
							continue;
						}
						else{//child process
							printf("%d,",(int)getpid());
							fflush(stdout);
							char* csvName = (char*)malloc(strlen(d->d_name)+1);
							strcpy(csvName, d->d_name);
							char *csvPath = createPath(csvName, startDirPath);
							/*(char*)malloc(strlen(csvName) + strlen(startDirPath) + 2);
							strcpy(csvPath, startDirPath);
							strcat(csvPath, "/");
							strcat(csvPath, csvName);
							*//*
							FILE *origin = fopen(csvPath, "r");
							if(origin == NULL){
								return 1;
							}
							free(csvPath);
							//pthread create

							int numColumns;
							Category* firstCat; 
							Row* firstRow = NULL;
							char* columns;
							char* buffer = NULL; size_t length = 0; int charRead = 0; //initialize variables for getline()
							charRead = getline(&buffer, &length, origin);//get line with categories
							if(!(strcmp(buffer, firstLine)) || charRead == -1){//if there is nothing in the file
								return 1;
							}
							columns = (char*)malloc(sizeof(char)*(charRead+1));
							strcpy(columns, buffer);
							buffer = strsep(&buffer, "\n"); // remove new line character for every row
							char* token;
							char* running = strdup(buffer);
							int count = 0; //count index postion
							if(running[0] == '"'){//check if first char is '"'
								strsep(&running, "\"");
							token = strsep(&running, "\"");
							strsep(&running, ",");
						}else{
							token = strsep(&running, ",");
						}
						trim(token);
						firstCat = (Category*)malloc(sizeof(Category));
						firstCat->name = (char*)malloc(sizeof(char)*(strlen(token)+1));
						strcpy(firstCat->name, token);

						firstCat->index = count;
						firstCat->dataType = dataArr[count];
						Category *ptr = firstCat;
						while(token != NULL && running != NULL){//check if theres no more tokens left
							ptr->nextCat = (Category*)malloc(sizeof(Category));
							if(running[0] == '"'){
								strsep(&running, "\"");
								token = strsep(&running, "\"");
								strsep(&running, ",");
							}else{
								token = strsep(&running, ",");
							}
							trim(token);
							count++;
							ptr->nextCat->name = (char*)malloc(sizeof(char)*(strlen(token)+1));
							strcpy(ptr->nextCat->name, token);
							
							ptr->nextCat->index = count;
							ptr->nextCat->dataType = dataArr[count];
							ptr = ptr->nextCat;
						}
						numColumns = count + 1;

						//read in rows


						charRead = getline(&buffer, &length, origin); //read next line
						firstRow = (Row*)malloc(sizeof(Row));
						firstRow->line = (char*)malloc(sizeof(char)*(strlen(buffer)+1));
						firstRow->info = (char**)malloc(sizeof(char*) * numColumns);
						strcpy(firstRow->line, buffer);
						buffer = strsep(&buffer, "\n");
						char* run = strdup(buffer);
						int numRow = 1;
						int currIndex = 0;
						for(currIndex; currIndex<numColumns && run != NULL; currIndex++){
							if(run[0] == '"'){//check if first char is '"'
								strsep(&run, "\"");
							token = strsep(&run, "\"");
							strsep(&run, ",");
							}else{
								token = strsep(&run, ",");
							}
							trim(token);
							firstRow->info[currIndex] = (char*)malloc(sizeof(char)*(strlen(token)+1));
							strcpy(firstRow->info[currIndex], token);
						}

						Row* rowPtr = firstRow; //pointer to first row
						charRead = getline(&buffer, &length, origin); //read second line
						while(charRead != -1){ //parse through each line in the file 
							rowPtr->nextRow = (Row*)malloc(sizeof(Row));
							rowPtr->nextRow->line = (char*)malloc(sizeof(char)*(charRead+1));
							rowPtr->nextRow->info = (char**)malloc(sizeof(char*)*numColumns);
							strcpy(rowPtr->nextRow->line, buffer);
							buffer = strsep(&buffer, "\n");
							run = (char*)realloc(run, strlen(buffer)+1);
							strcpy(run, buffer);
							
							currIndex = 0;
							for(currIndex; currIndex<numColumns && run != NULL; currIndex++){
								if(run[0] == '"'){//check if first char is '"'
									strsep(&run, "\"");
								token = strsep(&run, "\"");
								strsep(&run, ",");
							}else{
								token = strsep(&run, ",");
							}
								trim(token);
								rowPtr->nextRow->info[currIndex] = (char*)malloc(sizeof(char)*(strlen(token)+1));
								strcpy(rowPtr->nextRow->info[currIndex], token);
							}
							rowPtr = rowPtr->nextRow;
							numRow++;
							charRead = getline(&buffer, &length, origin);
						}
						rowPtr->nextRow = NULL;


							//user definied column to sort on

						char* userCat = argv[2];
						char type;
							int z; //index
							Category* traverse = firstCat; //traverse category list to get index postion of column to sort by and data type
							while(traverse != NULL){
								if(strcmp(traverse->name, userCat) == 0){
									type = traverse->dataType;
									z = traverse->index;
									break;
								}
								traverse = traverse->nextCat;
							}
							if(traverse == NULL){
								printf("\nFor PID: %d ,Column not found. Please enter proper parameters.\n", getpid());//error, column not found
								fflush(stdout);
								freeCatRecur(firstCat);
								freeRow(firstRow, numColumns);
								free(columns);
								return 1;
							}
							csvName = strsep(&csvName, ".");
							char *fullName = (char*)malloc(strlen(userCat) + strlen(csvName) + 13);
							strcpy(fullName, csvName);
							strcat(fullName, "-sorted-");
							strcat(fullName, userCat);
							strcat(fullName, ".csv");
							char* fullPath = fullName;
							if(outputDir != NULL){
								fullPath = createPath(fullName, outputDirPath);
								/*(char*)malloc(strlen(fullName) + strlen(outputDirPath) + 2);
								strcpy(fullPath, outputDirPath);
								strcat(fullPath, "/");
								strcat(fullPath, fullName);
								*//*
							}else{
								fullPath = createPath(fullName, startDirPath);
								/* (char*)malloc(strlen(fullName) + strlen(startDirPath) + 2);
								strcpy(fullPath, startDirPath);
								strcat(fullPath, "/");
								strcat(fullPath, fullName);
								*//*
							}
							FILE *file = fopen(fullPath, "w");
							firstRow = mergeSort(firstRow, numRow, z, type);
							fprintf(file, "%s", columns);
							Row* a = firstRow; //traverse list in order
							while(a != NULL){
								fprintf(file, "%s", a->line);
								a = a->nextRow;
							}
							fclose(origin);
							fclose(file);
							free(fullPath);
							free(csvName);
							freeCatRecur(firstCat);
							freeRow(firstRow, numColumns);
							free(columns);
							//if(!(strcmp(startDirPath, "."))){
							//	free(startDirPath);
							//}
							return 1;
						}
					}
					else{ //neither a csv file or directory
						continue;
					}
				}
				
			}

			closedir(startDir);
			int status, y;
			int count = 1;
			pid_t wpid; 
			while ((wpid = wait(&status)) > 0){
				if(WIFEXITED(status)){
					y = WEXITSTATUS(status);
					count = count + y;
				}
				
			}

			if(getpid() == ipid){
				printf("\nTotal number of processes: %d\n", count);
			}
			//if(!(strcmp(startDirPath, ".")) && getpid() != ipid){
			//	free(startDirPath);
			//}
			return count;
		}

		//incorrect input parameters
		else{
			printf("Incorrect inputs\n");
			return -1;
		}
	}

	//incorrect input parameters entered
	else{
		printf("Incorrect inputs\n");
		return -1;
	}*/

}

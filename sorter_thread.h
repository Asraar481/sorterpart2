#ifndef SORTER_H
#define SORTER_H

typedef struct rows{
	struct rows* nextRow;
	char** info;
	char* line;
}Row;
typedef struct categories Category;

int checkCSVorDirectory(char* fileName, char* currentDir);
char checkDataType(char* c);//only for extra credit
void trim(char *c);
int cmpRow(Row* r1, Row* r2, int index, char dataType);
Row* mergeSort(Row* head, int length, int index, char dataType);
int freeRow(Row* head, int numcolumns);
int freeCatRecur(Category* front);
char* createPath(char* fileName, char* directoryPath);

#endif //SORTER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sorter.h" //h file with row/col nodes

/*File contains implementation of merge sort on csv data
	mergeSort will require index of category, length of LL, and first row*/


/* typedef struct rows{
	struct rows* nextRow;
	char** info;
	char* line;
}Row; */

/*Helper method, to get to the last node in a Row LL of length given the first node; Used to break Row LL*/
Row* iterateToLastRow(Row* head, int length){
	int i;
	Row* ptr = head;
	for(i = 1;i < length; i++){
		if(ptr->nextRow)
			ptr = ptr->nextRow;
	}
	return ptr;
}

/*Helper method, to get to Column we are comparing on*/
/*Column* iterateToCol(Column* first, int index){
	int i;
	Column* ptr = first;
	for(i = 1;i < index; i++){
		if(ptr->nextCol)
			ptr = ptr->nextCol;
	}
	return ptr;
} */



//headerfile
/*Custom Compare method based on category adn datatype; index is Col # for comparison
	Datatype: i = int, d = double, s = string*/
int cmpRow(Row* r1, Row* r2, int index, char dataType){
	//Column* c1 = iterateToCol(r1->firstCol, index);
	//Column* c2 = iterateToCol(r2->firstCol, index);
	char* s1 = (r1->info)[index];
	char* s2 = (r2->info)[index];
	if(strlen(s1) == 0 || strlen(s2) == 0){
		if(strlen(s1) == 0)
			return -1;
		if(strlen(s2) == 0)
			return 1;
		return 0;
	}
	int diff = 0;
	int v1, v2;
	double d1, d2;
	double diff2 = 0.0;
	
	
	if(dataType == 'i'){
		v1 = atoi(s1);
		v2 = atoi(s2);
		diff = v1 - v2;
		if(diff < 0){
			return -1;
		}else{
			if(diff > 0){
				return 1;
			}else{
				return 0;
			}
		}
	}
	if(dataType == 'd'){
		d1 = atof(s1);
		d2 = atof(s2);
		diff2 = d1 - d2;
		//printf("%1.2f, %1.2f = %1.2f =", d1, d2, diff2);//debug
		if(diff2 < 0.0){
			return -1;
		}else{
			if(diff2 > 0.0){
				return 1;
			}else{
				return 0;
			}
		}
	}
	
	return strcmp(s1,s2);
	//cmp on str/int/float
	
	/* if(a == 'i' && b == 'i'){x = 1;}//Scenario 1: both values are ints
	else if(a == 'd' && b == 'd'){x = 2;}//Scenario 2: both values are doubles (floats)
		else if((a == 'd' && b == 'i')||(a == 'i' && b == 'd')){x = 3;}//Scenario 3: one value is double, other is int
				else if(a == 's' && b == 's'){x = 4;}//Scenario 4: both values are strings (char)
					else{//Return Cases, string always greater
						if(a == 's' && (b == 'i' || b == 'd')){return 1;}//Int or double less than string
						if((a == 'i' || a == 'd') && b == 's'){return -1;}//Int or double less than string
					} */
	
	//var for double, int, str s1,s2
	/* switch(x){
		case 1:
			
			return ()
			break;
		case 2:
			break;
		case 3:
			break;
		case 4:
			break;
		default:
	} */
}

//headerfile
/**Recursive mergesort, requires length of Row LL, head of Row LL, and Col index of category to compare on*/
Row* mergeSort(Row* head, int length, int index, char dataType){
	
	
	//base cases
	if(length == 0)
		return NULL;
	if(length == 1)
		return head;
	
	//split array/LL
	int odd = length % 2;//if odd length, odd=1
	int ll = length/2;//length of left side
	int rl = length/2 + odd;//length of right side, will be 1 longer if odd length
	Row* ptrBefore = iterateToLastRow(head, ll);
	Row* rightH = ptrBefore->nextRow;//head of right side
	ptrBefore->nextRow = NULL;//breaks LL into 2
	ptrBefore = iterateToLastRow(rightH, rl);
	ptrBefore->nextRow = NULL;
	
	
	//Recursion
	Row* left = mergeSort(head, ll, index, dataType);
	Row* right = mergeSort(rightH, rl, index, dataType);
	
	//merge
	Row* merged = NULL;//head of sorted list
	Row* mergedEnd = NULL;//end of list
	Row* ptr = NULL;//helps adding to list 
	
	while(left && right){
		if(cmpRow(left, right, index, dataType) > 0){
			ptr = right;
			right = right->nextRow;
		}else{
			ptr = left;
			left = left->nextRow;
		}
		if(merged == NULL){//append to mergedList
			merged = ptr;
			mergedEnd = ptr;
		}else{
			mergedEnd->nextRow = ptr;
			mergedEnd = mergedEnd->nextRow;
			mergedEnd->nextRow = NULL;
		}
		ptr = NULL;
	}
	//one of the sub arrays are gone
	while(left){
		ptr = left;
		left = left->nextRow;
		if(merged == NULL){
			merged = ptr;
			mergedEnd = ptr;
		}else{
			mergedEnd->nextRow = ptr;
			mergedEnd = mergedEnd->nextRow;
			mergedEnd->nextRow = NULL;
		}
		ptr = NULL;
	}
	while(right){
		ptr = right;
		right = right->nextRow;
		if(merged == NULL){
			merged = ptr;
			mergedEnd = ptr;
		}else{
			mergedEnd->nextRow = ptr;
			mergedEnd = mergedEnd->nextRow;
			mergedEnd->nextRow = NULL;
		}
		ptr = NULL;
	}
	
	//printf("mergeSort + %d\n", length);//debug
	return merged;
}





/* For sorter.c:
int freeCatRecur(Category* front){
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
//char **-from 0 to len of **, int columns
int freeRow(Row* head){
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
	return freeRow(ptr);
	return 0;
}

*/
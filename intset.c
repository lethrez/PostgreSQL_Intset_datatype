/*
 * src/tutorial/intset.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.


  Implementation could be optimized through an index wrapper of each individual
  integer. This would allow binary search when calling functions.
  The file is not incredibly optimized as it was a learning excercise for the 
  backend of postgreSQL.

  Created by Leo Hoare and Isabelle Lou
******************************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

PG_MODULE_MAGIC;


#define MAXDIGITSIZE 19

// structure of varlena defined here 
// could also just define intset as varlena....
// typedef struct varlena intset
typedef struct intset
{
	//int card;
	char vl_len_[4];
	char vl_dat[FLEXIBLE_ARRAY_MEMBER];
} intset;

// converts a set to cstring for operations
// make sure to free cstring after use
 
static char * intset_to_cstring(const intset *i)
 {
     int         len = VARSIZE(i)-VARHDRSZ;
     char       *result;
     result = (char *) palloc(len + 1);
     memcpy(result, VARDATA_ANY(i), len);
     result[len] = '\0';
     return result;
 }

// function to remove trailing and leading white space from tokens (or Cstrings).
static char *remove_space(char *str)
{
  char *end; 
  while(isspace((unsigned char)*str)) str++;
  if(*str == 0)
    return str;
  end = str + strlen(str) - 1;
  while(end > str && isspace((unsigned char)*end)) end--; 
  end[1] = '\0';
  return str;
}
// Quoted from https://stackoverflow.com/questions/122616/how-do-i-trim-leading-trailing-whitespace-in-a-standard-way

// Function finds the index of a character array in 
// used for insertion in intset data structure
// -2 means it is found the integer within the set 
// -1 means no number is greater than the new number 
// else if index > 0 it is the index of the comma before the number
static int find_index(intset *set, char *cstr){
	int32 num1= atol(cstr),num2,i=0,j=0,final_index=-1;
	char buffer[MAXDIGITSIZE]; 
	memset(buffer,'\0',MAXDIGITSIZE);
	for (i=0;i<(VARSIZE(set)-VARHDRSZ)+1;i++){
		if (i<VARSIZE(set)-VARHDRSZ && VARDATA(set)[i] != ',') {
			buffer[j] = VARDATA(set)[i];
			j++;
		}
		else{
			num2 = atol(buffer);
			if (num1 < num2) {
				final_index = i-j;
				return final_index;
			}
			else if (num1 == num2 ) {
				final_index = -2; 
				return final_index;
			}
			memset(buffer,'\0',MAXDIGITSIZE);		
			j=0;
		}
	}
	return final_index;
}


// function to concat the strings in ascending order
// and adds the comma between them
static intset *concat_inorder(intset *set, char *add){
    int32 index = find_index(set, add);
	int32 new_size;
	intset *new_intset;
	char *coma = ",";
	if (index >= -1){
		new_size = VARSIZE(set) + strlen(add) + 1;
        new_intset = (intset*)palloc(new_size); 	
		SET_VARSIZE(new_intset, new_size);
		if (index == -1){ // case for inserting at end
        		memcpy(VARDATA(new_intset), VARDATA(set), VARSIZE(set)-VARHDRSZ);
        		memcpy(VARDATA(new_intset)+VARSIZE(set)-VARHDRSZ, coma, 1); 
        		memcpy(VARDATA(new_intset)+VARSIZE(set)-VARHDRSZ+1, add, strlen(add));    
		}
		else { // case for insertion at index 
			memcpy(VARDATA(new_intset),VARDATA(set),index);
                        memcpy(VARDATA(new_intset)+index, add, strlen(add));
                        memcpy(VARDATA(new_intset)+index+strlen(add), coma, 1); 
                        memcpy(VARDATA(new_intset)+index+strlen(add)+1,VARDATA(set)+index,VARSIZE(set)-VARHDRSZ-index);
		}
		
		return new_intset;
	}
	else {return set;}	
}

// function to add brackets to intset

static intset * add_brackets(intset *set){
	int32 new_size = VARSIZE(set)+2;
	intset *new_intset = (intset*)palloc(new_size);
	char *open = "{", *close = "}";
	SET_VARSIZE(new_intset, new_size);
	memcpy(VARDATA(new_intset),open,1);
	memcpy(VARDATA(new_intset)+1,VARDATA(set),VARSIZE(set)-VARHDRSZ);
	memcpy(VARDATA(new_intset)+VARSIZE(set)-VARHDRSZ+1,close,1);
	return new_intset;
}

// function to remove brackets from intset

static intset * take_brackets(intset *set){
        int32 new_size = VARSIZE(set)-2;
        intset *new_intset = (intset*)palloc(new_size);
        SET_VARSIZE(new_intset, new_size);
        memcpy(VARDATA(new_intset),VARDATA(set)+1,VARSIZE(set)-VARHDRSZ-1);
        return new_intset;
}


// function to assign new intset given a character array

static intset* new_intset(intset * set, char *tok){
        set = (intset*)palloc(VARHDRSZ + strlen(tok));
        SET_VARSIZE(set,VARHDRSZ + strlen(tok)); 
        memcpy(VARDATA(set),tok,strlen(tok));
        return set;
}

// function to concat to end of character array

static intset *concat_intset(intset *set, char *add){
        int32 new_size = VARSIZE(set) + strlen(add) + 1;
        intset *new_intset = (intset*)palloc(new_size);
        char *coma = ",";
        SET_VARSIZE(new_intset, new_size);
        memcpy(VARDATA(new_intset), VARDATA(set), VARSIZE(set)-VARHDRSZ);
        memcpy(VARDATA(new_intset)+VARSIZE(set)-VARHDRSZ, coma, 1); 
        memcpy(VARDATA(new_intset)+VARSIZE(set)-VARHDRSZ+1, add, strlen(add));
        return new_intset;
}


// helper function to determine if the intset contains a number

static int does_contain(int32 num1, intset *set, int32 index){
        int32 num2,i=0,j=0;
        char buffer[MAXDIGITSIZE];
        memset(buffer,'\0',MAXDIGITSIZE);
        if( VARDATA(set)[1] == '}' ){ // empty set
                return 0;
        }
        for (i=index;i<(VARSIZE(set)-VARHDRSZ);i++){
                if (i<VARSIZE(set)-VARHDRSZ-1 && VARDATA(set)[i] != ',') {
                        buffer[j] = VARDATA(set)[i];
                        j++;
                }
                else{
                        // sets up new index (assumes always correctly formatted...)
                        num2 = atol(buffer);
                        if (num1 < num2 ){
                                return 0;
                        }
                        if (num1 == num2 ) {
                                return i;
                        }
                        memset(buffer,'\0',MAXDIGITSIZE);
                        j=0;
                }
        }
        return 0;
}



// helper function checks if all characters are the same in two intsets
static int does_equals(intset *set1, intset *set2){
        int32 len1 = VARSIZE(set1), len2= VARSIZE(set2), i;
        if (len1 != len2) { return 0; }
        else {
                for (i=1;i<len1-VARHDRSZ;i++){
                        if (VARDATA(set1)[i] != VARDATA(set2)[i]) {return 0;}
                }
        }
        return 1;
}


// helper function used for set difference and set disjunction
// produces a new intset which is the difference of set 1 - set 2
static intset *set_dif(intset *set1, intset *set2){
        intset *new_set=(intset*)palloc(VARHDRSZ);
        int32 i,j=0,current_index=1,new_index=0,loop_indicator=0;
        char buffer[MAXDIGITSIZE];
        // set new_set as empty
        SET_VARSIZE(new_set,VARHDRSZ);
        // cases for empty set
        if (VARDATA(set1)[1] == '}' || VARDATA(set2)[1] == '}'){
                return set1;
        }
        // otherwise start checking ints
        memset(buffer,'\0',MAXDIGITSIZE);
        for (i=1;i<VARSIZE(set1)-VARHDRSZ;i++){
                if (i<VARSIZE(set1)-VARHDRSZ-1 && VARDATA(set1)[i] !=','){
                        buffer[j]=VARDATA(set1)[i];
                        j++;
                }
                else {
                        new_index = does_contain(atol(buffer),set2,current_index);
                        if ( new_index != 0) { current_index = new_index; }
                        else {
                                // x doesn't contains the number
                                // concat to end of string
                                if ( loop_indicator == 0 ) { new_set = new_intset(new_set,buffer); loop_indicator++;}
                                else { new_set = concat_intset(new_set,buffer); }
                        }
                        memset(buffer,'\0',MAXDIGITSIZE);
                        j=0;
                }
        }
        new_set = add_brackets(new_set);
        return new_set;
}



/**********************************************************************
 * Input/Output functions
 *****************************************************************************/


// FUNCTION IN
/*
  The input function takes a cstring input from the user when creating the data type.
  The function checks for correct input, raising errors and not inserting if incorrect
  Implementing using Strtok and iterating based on coma seperated strings.
  An example of a correct input '{1,2,3,4,5}'
  The data type only accepts, white spaces (removed), '-', ',' or digits.
  MAXDIGITSIZE set to 19 as default.
  Due to the variable being 'TOASTED' and excess data being stored on overflow pages
  The variable supports large digit sizes.
  However, could be more efficient dealing with high cardinality (20,000+)
*/

PG_FUNCTION_INFO_V1(intset_in);

Datum
intset_in(PG_FUNCTION_ARGS)
{
	/* inserts into BST and then takes output string and stores in similar type to varlena */
	char *input1 = PG_GETARG_CSTRING(0);
	intset *result;
	char *input = remove_space(input1);
	char *tok = NULL,*rest, *nobrackets = (char*)palloc(strlen(input)-1);
	int i, loop_indicator=0;	
	result = (intset*)palloc(VARHDRSZ);
	SET_VARSIZE(result,VARHDRSZ);
	// remove brackets at position 1 or zero or cast error (implement later)
	if (input[0] != '{' || input[strlen(input)-1] != '}'){ ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),errmsg("CASE ERROR BRACKETS MUST BE START & END")));}
	else {strncpy(nobrackets,input+1,strlen(input)-2); nobrackets[strlen(input)-2] ='\0'; }

	rest = input;
	tok = strtok_r(nobrackets, ",", &rest);
	while ( tok != NULL){
		tok = remove_space(tok);	
		if (strlen(tok) != 0){
			for (i = 0; i < strlen(tok); i++){
				if (!isdigit(tok[i]) && !(tok[i] == '-' && i == 0)) {
					ereport(ERROR,(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),errmsg("CASE INVALID CHARACTERS IN STRING '%s' '%c'",tok,tok[i])));
				}
			}	
			if (strlen(tok) >= MAXDIGITSIZE) {
				ereport(ERROR,(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),errmsg("INTEGER TOO BIG, MUST BE LESS THAN %d",MAXDIGITSIZE)));
			}
			if (loop_indicator == 0){
				// make new intset
				result = (intset*)palloc(VARHDRSZ + strlen(tok)); //sizeof(int)
				SET_VARSIZE(result,VARHDRSZ + strlen(tok)); //sizeof(int)
				memcpy(VARDATA(result),tok,strlen(tok));
				loop_indicator++;
			}
			else {
				// concat current intset.
				result = concat_inorder(result,tok);
			}
		}
		tok = strtok_r(NULL,",",&rest);
	}
	result = add_brackets(result);
	PG_RETURN_POINTER(result);
}


/*
  Funciton out - presents how the data is represented to the user
  Simply turns the Intset (Varlena like) data type into Cstring
  Internal representation quite similar to string type.
*/

PG_FUNCTION_INFO_V1(intset_out);

Datum
intset_out(PG_FUNCTION_ARGS)
{	
	intset *out = (intset*)PG_GETARG_VARLENA_P(0);
	char *result = intset_to_cstring(out);
	PG_RETURN_CSTRING(result);
}


/*****************************************************************************
 * New Operators
  Operators could be made more efficient with implementation of data structures 
  and indexing structure wrapper on the intset data type.
  The time complexity of each structure could also further be improved
 *****************************************************************************/


/*
  Cardinality function
  Could be made much more efficient with indexing
  Simply return the amount of indexs.
  Returns an integer of cardinality
*/

PG_FUNCTION_INFO_V1(intset_card);

Datum
intset_card(PG_FUNCTION_ARGS){
	intset *set = (intset *)PG_GETARG_VARLENA_P(0);
	int32 card=0,i;
	if (VARDATA(set)[1] == '}'){PG_RETURN_INT32(card);}
	for (i=0;i<(VARSIZE(set)-VARHDRSZ);i++){
                if (VARDATA(set)[i] == ','){
			card++;
		}
	}
	card++;
	PG_RETURN_INT32(card);
}

/*
  Function to determine if the intset contains value
  Returns true or false.
*/

PG_FUNCTION_INFO_V1(intset_contains);

Datum
intset_contains(PG_FUNCTION_ARGS){
        int32 num1 = PG_GETARG_INT32(0);
		intset *set = (intset *)PG_GETARG_VARLENA_P(1);
        int32 num2,i=0,j=0;
        char buffer[MAXDIGITSIZE];
        memset(buffer,'\0',MAXDIGITSIZE);
        if( VARDATA(set)[1] == '}' ){ // empty set
		PG_RETURN_BOOL(0);		
	}
	for (i=1;i<(VARSIZE(set)-VARHDRSZ);i++){
                if (i<VARSIZE(set)-VARHDRSZ-1 && VARDATA(set)[i] != ',') {
                        buffer[j] = VARDATA(set)[i];
                        j++;
                }
                else{
                        num2 = atol(buffer);
                        if (num1 == num2 ) {
                                PG_RETURN_BOOL(1);
                        }
                        memset(buffer,'\0',MAXDIGITSIZE);
                        j=0;
                }
        }
        PG_RETURN_BOOL(0);
}


/*
  Function determine if the first intset is a subset of the second
  i.e. all elements in a are in b
*/


PG_FUNCTION_INFO_V1(intset_subset);

Datum
intset_subset(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
        int32 num,i=0,j=0,current_index=1;
        char buffer[MAXDIGITSIZE];
        memset(buffer,'\0',MAXDIGITSIZE);
        if (VARDATA(set1)[1] == '}') { // empty set
		PG_RETURN_BOOL(1);
	}	// empty set subset of all sets
        for (i=1;i<(VARSIZE(set1)-VARHDRSZ);i++){
                if (i<VARSIZE(set1)-VARHDRSZ-1 && VARDATA(set1)[i] != ',') {
                        buffer[j] = VARDATA(set1)[i];
                        j++;
                }
                else{
                        num = atol(buffer);
                        current_index = does_contain(num,set2,current_index);
			if (current_index == 0){ PG_RETURN_BOOL(0); }
                        memset(buffer,'\0',MAXDIGITSIZE);
                        j=0;
                }
        }
        PG_RETURN_BOOL(1);
}

/*
  Function to determine if both intsets are equal to each other
*/


PG_FUNCTION_INFO_V1(intset_equal);

Datum
intset_equal(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
	int32 len1 = VARSIZE(set1), len2= VARSIZE(set2), i;
	if (len1 != len2) { PG_RETURN_BOOL(0); }	
	else {
		for (i=1;i<len1-VARHDRSZ;i++){
			if (VARDATA(set1)[i] != VARDATA(set2)[i]) {PG_RETURN_BOOL(0);}	
		}
	}
	PG_RETURN_BOOL(1);
}

/*
  Function to determine if intsets are not equal to each other
  Complimentary function to equals
  Returns true or false
*/

PG_FUNCTION_INFO_V1(intset_not_equal);

Datum
intset_not_equal(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
        int32 len1 = VARSIZE(set1), len2= VARSIZE(set2);
        if (len1 != len2) { PG_RETURN_BOOL(1); }    
      	if (does_equals(set1,set2) == 1){ PG_RETURN_BOOL(0);}
        else { PG_RETURN_BOOL(1); }
}


/*
  Union between two intsets
  Returns a new intset that is union of the two inputs
*/

PG_FUNCTION_INFO_V1(intset_union);

Datum
intset_union(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
        intset *new_set=(intset*)palloc(VARHDRSZ), *x, *y;
        int32 i,j=0;
        char buffer[MAXDIGITSIZE], *result;
        // cases for empty set
        if (VARDATA(set1)[1] == '}'){
            result = intset_to_cstring(set2);
			PG_RETURN_CSTRING(result);
        }
        else if (VARDATA(set2)[1] == '}'){
			result = intset_to_cstring(set1);
            PG_RETURN_CSTRING(result);
        }
	
	memset(buffer,'\0',MAXDIGITSIZE);
	if ( VARSIZE(set1) > VARSIZE(set2) ){ x = set1; y = set2;}
	else { x = set2; y = set1;}
	new_set = take_brackets(x);
	for (i=1;i<VARSIZE(y)-VARHDRSZ;i++){
		if (i<VARSIZE(y)-VARHDRSZ-1 && VARDATA(y)[i] !=','){
			buffer[j]=VARDATA(y)[i];
			j++;
		}
		else {
			new_set = concat_inorder(new_set,buffer);
			//printf("Concatenated buffer is %s \n", intset_to_cstring(new_set));
			memset(buffer,'\0',MAXDIGITSIZE);
			j=0;
		}
	}
	//qsort(new_set, sizeof(new_set)/sizeof(*new_set), sizeof(*new_set), comp);
	new_set = add_brackets(new_set);
	result = intset_to_cstring(new_set);
	//printf("results is %s \n", result);
	PG_RETURN_CSTRING(result);
}


/*
  Intersection between two intsets
  Returns a new intset that is Intersection of the two inputs
*/


PG_FUNCTION_INFO_V1(intset_inters);

Datum
intset_inters(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
        intset *new_set=(intset*)palloc(VARHDRSZ), *x, *y; 
        int32 i,j=0,current_index=1,new_index=0,loop_indicator=0;
        char buffer[MAXDIGITSIZE], *result;
        // set new_set as empty
	SET_VARSIZE(new_set,VARHDRSZ);
	// cases for empty set
	if (VARDATA(set1)[1] == '}' || VARDATA(set2)[1] == '}'){
                new_set = add_brackets(new_set);
		result = intset_to_cstring(new_set);
		PG_RETURN_CSTRING(result);
        }
	// otherwise start checking ints
        memset(buffer,'\0',MAXDIGITSIZE);
        if ( VARSIZE(set1) > VARSIZE(set2) ){ x = set1; y = set2;}
        else { x = set2; y = set1;}
	for (i=1;i<VARSIZE(y)-VARHDRSZ;i++){
                if (i<VARSIZE(y)-VARHDRSZ-1 && VARDATA(y)[i] !=','){
                        buffer[j]=VARDATA(y)[i];
                        j++;
                }
                else {
                        new_index = does_contain(atol(buffer),x,current_index);
			if ( new_index != 0) {
				// x contains the number
				// update the largest number x has to loop from
				current_index = new_index;
				// concat to end of string
				if ( loop_indicator == 0 ) { new_set = new_intset(new_set,buffer); loop_indicator++;}
				else { new_set = concat_intset(new_set,buffer); }
			}
                        memset(buffer,'\0',MAXDIGITSIZE);
                        j=0;
                }
        }
	new_set = add_brackets(new_set);
        result = intset_to_cstring(new_set);
        PG_RETURN_CSTRING(result);
}



/*
  Difference between two intsets
  Returns a new intset that is elements in A not in B.
*/


PG_FUNCTION_INFO_V1(intset_dif);

Datum
intset_dif(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
	intset *new = set_dif(set1,set2);
	char *result;
	result = intset_to_cstring(new);
	PG_RETURN_CSTRING(result);
}



/*
  Difference between two intsets
  Returns a new intset that is elements in A not in B and B but not A
*/

PG_FUNCTION_INFO_V1(intset_disj);

Datum
intset_disj(PG_FUNCTION_ARGS){
        intset *set1 = (intset *)PG_GETARG_VARLENA_P(0);
        intset *set2 = (intset *)PG_GETARG_VARLENA_P(1);
        intset *final,*new1, *new2, *x , *y;
	char *result, buffer[MAXDIGITSIZE];
        int32 i=1,j=0;
	if (VARDATA(set1)[1] == '}') { final = set2; }
	else if (VARDATA(set2)[1] == '}') { final = set1; } 
	else {
		new1 = set_dif(set1,set2);
		new2 = set_dif(set2,set1);
		if (VARDATA(new1)[1] == '}'){
                	result = intset_to_cstring(new2);
                	PG_RETURN_CSTRING(result);
        	}
        	else if (VARDATA(new2)[1] == '}'){
                	result = intset_to_cstring(new1);
                	PG_RETURN_CSTRING(result);
        	}
        	memset(buffer,'\0',MAXDIGITSIZE);
        	if ( VARSIZE(new1) > VARSIZE(new2) ){ x = new1; y = new2;}
        	else { x = new2; y = new1;}
        	final = take_brackets(x);
        	for (i=1;i<VARSIZE(y)-VARHDRSZ;i++){
                	if (i<VARSIZE(y)-VARHDRSZ-1 && VARDATA(y)[i] !=','){
                        	buffer[j]=VARDATA(y)[i];
                        	j++;
                	}
                	else {
                        	concat_intset(final,buffer);
                       		memset(buffer,'\0',MAXDIGITSIZE);
                        	j=0;
                	}
        	}
        	final = add_brackets(final);
        }
	result = intset_to_cstring(final);
        PG_RETURN_CSTRING(result);
}

    

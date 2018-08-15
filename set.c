#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>



// node structure for bst
typedef struct node
{
	int key;
	struct node *left, *right;
} node;

// bst structure to point to root
typedef struct bst
{
	node *root;
} bst;

// helper function to create new node
node *newNode(int value)
{
	node *tmp = (node*)malloc(sizeof(node));
	if (tmp == NULL){printf("WHAT TO DO HERE??");}
	tmp->key = value;
	tmp->right = tmp->left = NULL;
	return tmp;
}


// function to create new bst
bst *newbst()
{
	bst *tmp = (bst*)malloc(sizeof(bst));
	if (tmp == NULL){printf("WHAT TO DO HERE?");}
	tmp->root = NULL;
	return tmp;
}

// insert function for bst
node* insert(bst *tree, node *node, int value)
{
	if (!tree->root){tree->root = newNode(value); return tree->root;}
	else 
	{
		if (node == NULL) { node = newNode(value);}
		else if (value < node->key) {node->left = insert(tree,node->left,value);}
		else if (value > node->key) {node->right = insert(tree,node->right,value);}
		return node;
	}
}

// function to inorder print tree (could use to make set string later?)
void printtree(node* node)
{
	if (node == NULL){return;}
	printtree(node->left);
	printf("%i,",node->key);
	printtree(node->right);
}


int main()
{
	char str[] = "1,-23,341,2,532,1  ,    2 ,,1";
	char *tok = NULL, *fixed = str;
	tok = strtok_r(str, ",", &fixed);
	bst *tree = newbst();
	while (tok != NULL)
	{
		// loops through and checks all characters are digits, numbers or white space
		for (int i=0; tok[i]!=0; i++)
 		{if (!isdigit(tok[i]) && tok[i] != ' ' && tok[i] != '-') {printf("INCORRECT INPUT %c",tok[i]);}}
	
		// assumes valid input ^^ maybe should exit at this point beforehand if not
		insert(tree,tree->root,atoi(tok));
			
		// takes next comma seperated string 
		tok = strtok_r(NULL, ",", &fixed);
	}
	printtree(tree->root);
	return 0;
}





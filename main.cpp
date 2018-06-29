/* 
 * File:   main.cpp
 * Author: Tasos
 *
 * Created on April 17, 2015, 1:09 PM
 */

#include "dbtproj.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>
using namespace std;

void GenerateFile(FILE *outfile, int nblocks);

int main(int argc, char** argv) {
        
    // generate one or two files depends on the function
    FILE *gen1;
    gen1 = fopen ("file1.bin","wb");
    GenerateFile(gen1,NUM_OF_BLOCKS);

    /*FILE *gen2;
    gen2 = fopen ("file2.bin","wb");
    GenerateFile(gen2,NUM_OF_BLOCKS-5);*/

    // Initialize variables for calling the functions below
    char infile[10] = "file1.bin";
    char outfile[11] = "output";
    unsigned char field = '1';
    unsigned int nmem_blocks = 3;
    block_t *buffer = new block_t[nmem_blocks];
    unsigned int *nsorted_segs = new unsigned int;
    unsigned int *npasses = new unsigned int;
    unsigned int *nios = new unsigned int;
    unsigned int *nunique = new unsigned int;
    unsigned int *nres = new unsigned int;
    (*nsorted_segs) = 0;
    (*npasses) = 0;
    (*nios) = 0;
    (*nunique) = 0;
    (*nres) = 0;

    /*~~~~~~~~~~~~~~~~ Choose and run the function below ~~~~~~~~~~~~~~*/
    //MergeSort(infile,field,buffer,nmem_blocks,outfile,nsorted_segs,npasses,nios);
    //EliminateDuplicates("output.bin",field,buffer,nmem_blocks,"eliminate.txt",nunique,nios);
    //MergeJoin("file1.bin","file2.bin",field,buffer,nmem_blocks,"outMergeJoin.txt",nres,nios);
    //HashJoin("file1.bin","file2.bin",field,buffer,nmem_blocks,"outHashJoin.txt",nres,nios);



    // Call function for MergeSort, EliminateDuplicates, MergeJoin
    /*int option = 2;
    if (option==1){
        cout << "Which field would you like to be sorted in the file?" <<endl;
        cout << "For RecID press 0, for number press 1, for string press 2 and for number-string press 3.";
        cin >> field;
        MergeSort(infile,field,buffer,nmem_blocks,outfile,nsorted_segs,npasses,nios);
    }
    else if (option==2){
        cout << "Which field would you like to erase the duplicates?" <<endl;
        cout << "For RecID press 0, for number press 1, for string press 2 and for number-string press 3.";
        cin >> field;
        EliminateDuplicates("output.bin",field,buffer,nmem_blocks,"eliminate.txt",nunique,nios);
    }
    else if (option==3){
        MergeJoin("file1.bin","file2.bin",field,buffer,nmem_blocks,"out_join.txt",nres,nios);
    }
    else{
        cout << "Wrong Choice. Try again"
        flag = false;
    }*/

    cout << endl << "All executions finished. Statisctic Results:" << endl;
    cout << "Total Sorted Segments: " << *nsorted_segs << endl;
    cout << "Total Merge Sort Passes: " << *npasses << endl;
    cout << "Total I/Os: " << *nios << endl;
    cout << "Unique Records: " << *nunique << endl;
    cout << "Total Pairs: " << *nres << endl;



    // Delete the variables from memory
    delete[] buffer;
    delete nsorted_segs;
    delete npasses;
    delete nios;
    delete nunique;

    return 0;
}


void GenerateFile(FILE *outfile, int nblocks)
{
    // initialize stuff
    record_t record;
    block_t *block;
    unsigned int recid = 0;
    int last_percentage;

    srand(time(NULL));

    cout << "Generating input file with " << nblocks << " blocks...   0%  " << flush;

    for (int b = 0; b<nblocks; b++) { // for each block
        block = new block_t;
        block->blockid = b;
        for (int r = 0; r<MAX_RECORDS_PER_BLOCK; r++) { // for each record
            
            // prepare a record
            record.recid = recid++;
            record.num = rand() % MAX_RECORD_NUM;
            strcpy(record.str, "hello");   // put the same string to all records
            record.valid = true;

            //memcpy(block->entries[r], &record, sizeof(record_t)); // copy record to block
            block->entries[r] = record;
        }

        block->nreserved = MAX_RECORDS_PER_BLOCK;
        block->valid = true;

        fwrite(block, 1, sizeof(block_t), outfile);	// write the block to the file
        delete block;


        if(last_percentage != (int) (((double)b*100)/nblocks)) {
            last_percentage = (int) (((double)b*100)/nblocks);
            cout << "\rGenerating input file with " << nblocks << " blocks...   " << last_percentage << "%  " << flush;
        }
    }

    cout << "\rGenerating input with " << nblocks << " blocks...   100%  " << endl;
    fclose(outfile);
}


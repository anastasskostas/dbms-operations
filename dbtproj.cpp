#include "dbtproj.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>

using namespace std;
void PrintMergeSortResults(char *file);
int compareRecid (const void * a, const void * b);
int compareNum(const void * a, const void * b);
int compareStr (const void * a, const void * b);
int compareNumStr (const void * a, const void * b);
int min(int a,int b);
void pushToLastBlock(record_t rec,block_t *block, char *outfile,int unsigned *nios);
int fieldCompare(record_t rec1, record_t rec2, unsigned char field);
int hFunction(record_t record,unsigned char field);

int buffer_record_size;

void MergeSort (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios){
    int f = field - 48;

    // Initialize the file names
    char SEGMENTS_FILE[100];
    strcpy(SEGMENTS_FILE,outfile);
    strcat(SEGMENTS_FILE,SEG1);
    char SEGMENTS_FILE_2[100];
    strcpy(SEGMENTS_FILE_2,outfile);
    strcat(SEGMENTS_FILE_2,SEG2);

    char outbin[100];
    strcpy(outbin,outfile);
    strcat(outbin,".bin");
    
    // Delete previous segments file, if there is one
    FILE *del = fopen(SEGMENTS_FILE,"w");
    fwrite(NULL,0,0,del);
    fclose(del);

    // Delete previous output file, if there is one
    del = fopen(outbin,"w");
    fwrite(NULL,0,0,del);
    fclose(del);

    // Initialize variables
    block_t *block = new block_t;
    int number_of_blocks,number_of_segments=0;
    long read_counter=0,last_read_counter=0;
    int p=0;
    FILE *segments_file;
    FILE *input = fopen(infile, "rb");
    
    
    //Obtain the file size
    fseek(input, 0L, SEEK_END);
    number_of_blocks = ftell(input)/sizeof(block_t);
    cout << "Input file has " << number_of_blocks << " blocks with " << MAX_RECORDS_PER_BLOCK << " records each, to be sorted by ";
    
    switch(f){
        case 0:
            cout << "recid" << endl;
            break;
        case 1:
            cout << "number" << endl;
            break;
        case 2:
            cout << "string" << endl;
            break;
        case 3:
            cout << "number & string" << endl;
            break;
    }
    
    fseek(input, 0L, SEEK_SET);

    cout << "Reading the input."<< endl;
    cout << "Creating and Sorting Initial Segments...  0%  " << flush;
    
    if (number_of_blocks <= nmem_blocks){
        segments_file = fopen(outbin,"ab");
    }
    else{
        segments_file = fopen(SEGMENTS_FILE,"ab");
    }
    
    // Read all the blocks, while end-of-file has not been reached
    while (!feof(input)){
        // Print the %
        if (last_read_counter != (long)(((double)read_counter*100)/((double)number_of_blocks))){
            last_read_counter = (long)(((double)read_counter*100)/((double)number_of_blocks));
            cout << "\rCreating and Sorting Initial Segments...  " << last_read_counter << "%  " << flush;
        }
        read_counter++;

        // Read the next block
	fread(block, 1, sizeof(block_t), input);
        (*nios)++;

        // Add the block to the buffer
        if (!feof(input)){
            buffer[p++] = *block;
        }

        // If the buffer is full, or the file has ended...
        if (p == nmem_blocks || (feof(input) && p>0)){
            // Create a new table to hold all the records for quicksorting
            record_t *records = new record_t[p*MAX_RECORDS_PER_BLOCK];

            for (int i=0 ; i<p ; i++){
                for(int j=0 ; j<buffer[i].nreserved ; j++){
                    records[i*MAX_RECORDS_PER_BLOCK + j] =  buffer[i].entries[j];
                }
            }


            // Quicksort the table with the records
            switch (f){
                case 0:
                    qsort(records,p*MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareRecid); 
                    break;
                case 1:
                    qsort(records,p*MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareNum); 
                    break;
                case 2:
                    qsort(records,p*MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareStr); 
                    break;
                case 3:
                    qsort(records,p*MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareNumStr); 
                    break;
            }

            // Set the records back to the buffer blocks
            for (int i=0 ; i<p ; i++){
                for (int j=0 ; j<buffer[i].nreserved ; j++){
                    buffer[i].entries[j] = records[i*MAX_RECORDS_PER_BLOCK + j];
                }
            }


            // Write the segment's size
            fwrite(&p,1,sizeof(int),segments_file);
            (*nios)++;

            // Write each block
            for (int i=0 ; i<p ; i++){
                fwrite(&buffer[i],1,sizeof(block_t),segments_file);
                (*nios)++;
            }
            (*nsorted_segs)++;

            // Change the counters
            number_of_segments++;
            p=0;

            delete[] records;
        }
    }
    
    // Add a pass cause of quicksorting
    (*npasses)++;
    
    cout << "\rCreating and Sorting Initial Segments...  100%" << endl;
    cout << "Wrote " << number_of_segments << " segments." << endl;
    
    // Close all the files
    fclose(input);
    fflush(segments_file);
    fclose(segments_file);

    segments_file = NULL;
    
    
    // Initialize more stuff for the merge sort algorithm
    int number_of_iterations = 1;
    int current_segment_batch = 0;
    int new_segment_file_counter = number_of_segments;
    bool segments_file_ended = false;
    bool second_segment_file = false;
    FILE *new_segments_file;
    
    // Start the Merge Sort Algorithm
    while (new_segment_file_counter > 1){
        cout << endl << "@Merge Sort Algorithm Iteration #" << number_of_iterations << endl;

        int number_of_current_segment_file_iterations = 1;
        new_segment_file_counter = 0;

        // Delete old/used segment files
        FILE *del;
        if (!second_segment_file){
            del = fopen(SEGMENTS_FILE_2,"w");
        }
        else{
            del = fopen(SEGMENTS_FILE,"w");
        }

        fwrite(NULL,0,0,del);
        fclose(del);

        int total_blocks=0;

        // Loop for all the segment batches (if the segments number is greater than the buffer blocks)
        while (!segments_file_ended){
            cout << " @Current Segments File Iteration #" << number_of_current_segment_file_iterations << endl;

            int max_blocks_in_buffer = min(number_of_segments,nmem_blocks-1); // The max blocks that the buffer can hold at the same time
            int new_segment_block_index = 0; // Holds the number of blocks the new segment
            block_t *temp_block = new block_t; // Temp variable to use it for cashing

            int *current_record_index = new int[max_blocks_in_buffer]; // The pointer to the current record in each block
            int *current_block_in_segment_index = new int[max_blocks_in_buffer]; // The pointer to the current block in each segment
            int output_record_index = 0; // The pointer to the current record of the last buffer block, which is used to hold the sorted block

            // Temp variables for the comparing
            int min_record_recid;
            int min_record_num;
            int min_record_block_index = -1;
            char *min_record_string = new char[10];

            // The variable that holds the remaining segments from the current segment batch
            int remaining_segments;

            // Holds the total blocks of the current segment batch, used for fseeking
            int current_total_blocks=0;

            // Variables for the % printing
            long comparing_counter=0;
            long last_comparing_counter=0;

            cout << "   -Max blocks in buffer is " << max_blocks_in_buffer << endl;

            if (!second_segment_file){
                cout << "   -Reading segments file \""<< SEGMENTS_FILE_2 << "\" " << endl;
            }
            else{
                cout << "   -Reading segments file \""<< SEGMENTS_FILE << "\" " << endl;
            }


            // Open the correct segments file
            if (!second_segment_file){
                segments_file = fopen(SEGMENTS_FILE,"rb");
            }
            else{
                segments_file = fopen(SEGMENTS_FILE_2,"rb");
            }

            // Seek to the next segment batch, if this segment file has more segments that the buffer can hold at the same time.
            fseek(segments_file,current_segment_batch*(nmem_blocks-1)*sizeof(int) + total_blocks*sizeof(block_t),SEEK_SET);

            cout << "   -Setting the first block of each segment to the buffer..." << endl;
            //cout << "   -Current segments: " << endl;

            // Set the first block of each segment to the buffer blocks, in order to start the comparison
            int u=0,seg_size=1;
            while (u<max_blocks_in_buffer && !feof(segments_file)){
                // Seek to the correct segment each time
                fseek(segments_file,(seg_size-1)*sizeof(block_t) ,SEEK_CUR);

                if (!feof(segments_file)){
                    // Read the segment's size
                    fread(&seg_size,1,sizeof(int),segments_file);
                    (*nios)++;
                    // Read the first block
                    fread(temp_block,1,sizeof(block_t),segments_file);
                    (*nios)++;

                    // Put it into the buffer
                    buffer[u] = *temp_block;

                    // Count it's blocks to the total
                    current_total_blocks+=seg_size;

                    //cout << "    +Segment #"<< u << " | Blocks: " << seg_size << endl;
                }

                u++;
            }


            // Initialize the indexes
            for (int i=0 ; i<max_blocks_in_buffer ; i++){
                current_record_index[i] = 0;
                current_block_in_segment_index[i] = 1;
            }


            // Check if the current segments file has ended, in order to switch after this comparison
            if (feof(segments_file) || max_blocks_in_buffer < nmem_blocks -1 || number_of_segments == max_blocks_in_buffer){
                segments_file_ended = true;
            }

            // Open the new segments file to write each sorted block
            // If the current total blocks are equal with the total blocks, then we are at the last merge pass, so, write the result to the outfile.bin
            if (current_total_blocks == number_of_blocks){
                // Delete any previous
                FILE *del = fopen(outbin,"w");
                fwrite(NULL,0,0,del);
                fclose(del);

                new_segments_file = fopen(outbin,"ab");
            }
            else{ // Else write to the seg1 or seg2 files
                if (!second_segment_file){
                    new_segments_file = fopen(SEGMENTS_FILE_2,"ab");
                }
                else{
                    new_segments_file = fopen(SEGMENTS_FILE,"ab");
                    //cout << "   -Buffer is empty. Writing Segment with nblocks " << new_segment_block_index << " to file " << SEGMENTS_FILE << endl;
                }
            }
            // Write the segment's size
            fwrite(&current_total_blocks,1,sizeof(int),new_segments_file);
            (*nios)++;

            // Set the remaining segments variable to the max blocks
            remaining_segments = max_blocks_in_buffer;

            cout << "   -Total segments: " << remaining_segments << endl;
            cout << "   -Comparing records...   0%  ";

            // The loop for the current segment batch / blocks left in the buffer
            // This loop will create a sorted segment, which contains all the current segments
            while (remaining_segments >0){
                // Print the %
                if (last_comparing_counter != (long) (((double)comparing_counter*100) / ( (double)current_total_blocks*MAX_RECORDS_PER_BLOCK))){
                    last_comparing_counter = (long) (((double)comparing_counter*100) / ( (double)current_total_blocks*MAX_RECORDS_PER_BLOCK));
                    cout << "\r   -Comparing records...   " << last_comparing_counter << "%  " << flush;
                }
                comparing_counter++;

                // Initialize the min record block index
                min_record_block_index = -1;

                // The loop for comparing the values
                for (int k = 0; k<max_blocks_in_buffer ; k++){
                    if (current_record_index[k] < MAX_RECORDS_PER_BLOCK){ //If the block still has records to be compared
                        if (min_record_block_index == -1){ // Initiate the min variables
                            min_record_block_index = k;
                            min_record_recid = buffer[k].entries[current_record_index[k]].recid;
                            min_record_num = buffer[k].entries[current_record_index[k]].num;
                            strcpy(min_record_string, buffer[k].entries[current_record_index[k]].str);
                        }

                        switch (f){
                            // Compare recid
                            case 0:
                                if(buffer[k].entries[current_record_index[k]].recid < min_record_recid ){
                                    min_record_block_index = k;
                                    min_record_recid = buffer[k].entries[current_record_index[k]].recid;
                                }
                                break;
                            // Compare number
                            case 1:
                                if(buffer[k].entries[current_record_index[k]].num < min_record_num){
                                    min_record_block_index = k;
                                    min_record_num = buffer[k].entries[current_record_index[k]].num;
                                }
                                break;
                            // Compare string
                            case 2:
                                if(strcmp(buffer[k].entries[current_record_index[k]].str,min_record_string) < 0){
                                    min_record_block_index = k;
                                    strcpy(min_record_string,buffer[k].entries[current_record_index[k]].str);
                                }
                                break;
                            // Compare number & string
                            case 3:
                                if(buffer[k].entries[current_record_index[k]].num < min_record_num){
                                    min_record_block_index = k;
                                    min_record_num = buffer[k].entries[current_record_index[k]].num;
                                    strcpy(min_record_string,buffer[k].entries[current_record_index[k]].str);
                                }
                                else if(buffer[k].entries[current_record_index[k]].num == min_record_num && strcmp(buffer[k].entries[current_record_index[k]].str,min_record_string) < 0){
                                    min_record_block_index = k;
                                    min_record_num = buffer[k].entries[current_record_index[k]].num;
                                    strcpy(min_record_string,buffer[k].entries[current_record_index[k]].str);
                                }
                                break;
                        }
                    }
                }


                // Pass the min record to the next empty slot of the last block of the buffer
                buffer[max_blocks_in_buffer].entries[output_record_index++] = buffer[min_record_block_index].entries[current_record_index[min_record_block_index]++];

                // If the last output-block is full...
                if (output_record_index == MAX_RECORDS_PER_BLOCK){
                    // Write the last block to the new segments file
                    fwrite(&buffer[max_blocks_in_buffer],1,sizeof(block_t),new_segments_file);
                    (*nios)++;
                    output_record_index = 0;
                }

                // Check if the min value block in the buffer is empty (after the comparison), in order to replace it
                if (current_record_index[min_record_block_index] == MAX_RECORDS_PER_BLOCK){
                    // Seek to the current segment batch
                    fseek(segments_file,current_segment_batch*(nmem_blocks-1) * sizeof(int) + total_blocks*sizeof(block_t),SEEK_SET ) ;

                    // Seek to the current segment
                    int seg_size;
                    for (int i=0 ; i<min_record_block_index ; i++){
                        fread(&seg_size,1,sizeof(int),segments_file);
                        (*nios)++;

                        fseek(segments_file,seg_size*sizeof(block_t),SEEK_CUR);
                    }

                    // Read the current segment's size
                    fread(&seg_size,1,sizeof(int),segments_file);

                    // If the segment still has more blocks to compare...
                    if (seg_size  > current_block_in_segment_index[min_record_block_index]){
                        // Replace the finished block with a new one, reading it from the correct segment
                        fseek(segments_file,current_block_in_segment_index[min_record_block_index]*sizeof(block_t),SEEK_CUR);
                        fread(&buffer[min_record_block_index],1,sizeof(block_t),segments_file);
                        (*nios)++;

                        current_record_index[min_record_block_index] = 0;
                        current_block_in_segment_index[min_record_block_index]++;
                    }
                    else{ // Else lower the remaining_segments counter;
                        remaining_segments--;
                    }
                }
            }

            // Close the segments file
            fclose(segments_file);

            cout << "\r   -Comparing records...   100%" << endl;

            (*nsorted_segs)++;
            fflush(new_segments_file);
            fclose(new_segments_file);

            // And change all the appropriate variables
            total_blocks += current_total_blocks;

            new_segment_block_index = 0;
            new_segment_file_counter++;
            current_segment_batch++;
            number_of_segments -= max_blocks_in_buffer;

            number_of_current_segment_file_iterations++;

            // Delete variables
            delete[] current_record_index;
            delete[] current_block_in_segment_index;
            delete[] min_record_string;

        }

        cout << " -Switching segment files and proceeding to the next iteration, if there is one." << endl;

        // Change segments file
        second_segment_file = !second_segment_file;

        // Change the variables
        segments_file_ended = false;
        number_of_segments = new_segment_file_counter;
        current_segment_batch = 0;
        number_of_iterations++;

        (*npasses)++;
        
        // Cleaning again
        delete block;

        // file=0 means results are in the "segments.bin, and 1 means "new_segments.bin"


        // Print the sorted records to the outfile
        PrintMergeSortResults(outfile);

        // Return the file that has the whole segment
    }
}

void EliminateDuplicates (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios){
    // Initilize variables
    int seg_size;
    block_t *block  = new block_t;
    record_t *last_record = new record_t;

    int f = field - 48;
    
    // Open input/output
    FILE *input = fopen(infile, "r");
    FILE *output = fopen(outfile,"w");

    // Read the size of the one huge segment
    fread(&seg_size,1,sizeof(int),input);
    (*nios)++;

    // Usefull variable for the comparison
    bool write;

    cout << endl << "Starting the Eliminate Duplicates Algorithm at the input " << infile << endl;

    // Loop for all the blocks
    for (int i=0 ; i < seg_size ; i++){
        // Read the next block
        fread(block,1,sizeof(block_t),input);
        (*nios)++;

        // Loop for all the records
        for (int j=0 ; j < MAX_RECORDS_PER_BLOCK ; j++){
            write = false;

            // Initialize the last record variable if it's the first record
            if (i==0 && j==0){
                *last_record = block->entries[0];
                fprintf(output,"Block #%d  recid: %d  number: %d  str: %s \n",i,last_record->recid,last_record->num, last_record->str);
                (*nios)++;
                (*nunique)++;
            }
            else{ // Else compare the current record with the last one
                switch(f){
                    case 0: // Compare recid
                        if(last_record->recid != block->entries[j].recid){
                            write = true;
                        }
                        break;
                    case 1: // Compare num
                        if(last_record->num != block->entries[j].num){
                            write = true;
                        }
                        break;
                    case 2: // Compare str
                        if(strcmp(last_record->str, block->entries[j].str) != 0 ){
                            write = true;
                        }
                        break;
                    case 3: // Compare num & str
                        if(last_record->num != block->entries[j].num && strcmp(last_record->str, block->entries[j].str) != 0){
                            write = true;
                        }
                        break;
                }

                // Set the last record variable to the current record
                *last_record = block->entries[j];

                // If the current record is different than the last, write it into the output
                if (write){
                    fprintf(output,"Block #%d  recid: %d  number: %d  str: %s \n",i,last_record->recid,last_record->num, last_record->str);
                    (*nunique)++;
                    (*nios)++;
                }
            }
        }
    }

    // Close stuff
    fclose(input);
    fflush(output);
    fclose(output);

    cout << endl << "Finished Eliminating Duplicates. Check results in " << outfile << endl;
}

void MergeJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios){
    // Initialize names
    char small_file_name[100];
    char big_file_name[100];
    int small_size,big_size,first_size,second_size;

    // Initialize counters
    unsigned int *nsorted_segs = new unsigned int;
    unsigned int *npasses = new unsigned int;

    // Delete previous output file, if there is one
    FILE *del = fopen(outfile,"w");
    fwrite(NULL,0,0,del);
    fclose(del);


    // Merge sort bot of the files
    MergeSort(infile1,field,buffer,nmem_blocks,"outfile1",nsorted_segs,npasses,nios);
    MergeSort(infile2,field,buffer,nmem_blocks,"outfile2",nsorted_segs,npasses,nios);

    cout << endl << endl << "Starting Merge Join Algorithm" << endl;

    buffer[nmem_blocks-1].nreserved=0;

    // Open the sorted files
    FILE *small_sorted_file,*big_sorted_file;
    FILE *merged_file_1 = fopen("outfile1.bin","r");
    FILE *merged_file_2 = fopen("outfile2.bin", "r");

    // Read their size
    fread(&first_size,1,sizeof(int),merged_file_1);
    fread(&second_size,1,sizeof(int),merged_file_2);
    (*nios)+=2;
    
    // Find which is small and which is big
    if(first_size < second_size){
        big_sorted_file = merged_file_2;
        strcpy(big_file_name,infile2);
        big_size = second_size;

        small_sorted_file = merged_file_1;
        strcpy(small_file_name,infile1);
        small_size = first_size;
    }
    else{
        big_sorted_file = merged_file_1;
        strcpy(big_file_name,infile1);
        big_size = first_size;

        small_sorted_file = merged_file_2;
        strcpy(small_file_name,infile2);
        small_size = second_size;

    }

    cout << "Small file is " << small_file_name << " with " << small_size << " blocks" << endl;
    cout << "Big file is " << big_file_name << " with " << big_size << " blocks" << endl;


    // Initialize variables
    int i=0,j=0;
    int compare;
    
    //read first block from each file
    fread(&buffer[0],1,sizeof(block_t), small_sorted_file);
    fread(&buffer[nmem_blocks-2],1,sizeof(block_t), big_sorted_file);
    (*nios)+=2;
    
    //cout << &buffer[1] << " dsadada " << endl;
    
    // Main Algorithm
    // While neither of the files has reached the end
    while (i < big_size*MAX_RECORDS_PER_BLOCK && j < small_size*MAX_RECORDS_PER_BLOCK ){
        // Compare the current record of the first block, which is from the small file
        // with the current record on the nmem_blocks-2 block, which is from the big file
        compare = fieldCompare(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],buffer[0].entries[j%MAX_RECORDS_PER_BLOCK],field);
        
        // If their field is equal
        if (compare == 0){
            int current_iterations=0; // Counts the whole iterations
            int current_j_block=0; // In which position in the buffer, the last block of the small file was read
            int jj; // Temp counter

            // This is the part in which the algorithm checks all the possible matches, in case there are more rows with the same field in each file
            // While the big file hasn't ended
            while (compare==0 && i<big_size*MAX_RECORDS_PER_BLOCK){
                jj=j; // Set the jj variable equal to j
                current_j_block=0; // Reset the current j block

                // While the small file hasn't ended
                while (compare==0 && jj<small_size*MAX_RECORDS_PER_BLOCK){
                //cout << i << " - " << jj << endl;

                    // If jj has passed the maximum records per block, then it needs to bring another block from the small file to the buffer
                    if (jj % MAX_RECORDS_PER_BLOCK == 0 && jj!=j){
                        // Raise the current j block counter
                        current_j_block++;
                        //cout << i << "-" << jj << "-"<< current_j_block << "  " << flush;
                        
                        // If the buffer has more space to put the new block (cause the last and the one before last are reserved)
                        if (current_j_block < nmem_blocks - 3){
                            // If it is the first iteration, cause if it was not the first, the blocks should already be readen
                            if (current_iterations == 0){
                                // Seek to that block and read it
                                fseek(small_sorted_file,sizeof(int) + (jj/MAX_RECORDS_PER_BLOCK)*sizeof(block_t),SEEK_SET );
                                fread(&buffer[current_j_block],1,sizeof(block_t),small_sorted_file);
                                (*nios)++;
                                //cout << " Small read block j" << endl;
                            }
                        }
                        // If the buffer does not have enough blocks, then use the last remaining block (nmem_blocks - 3) for each read block
                        else{
                            current_j_block = nmem_blocks - 3;
                            fseek(small_sorted_file,sizeof(int) + (jj/MAX_RECORDS_PER_BLOCK)*sizeof(block_t),SEEK_SET );
                            fread(&buffer[current_j_block],1,sizeof(block_t),small_sorted_file);
                            (*nios)++;
                        }
                    }


                    // Write the pair to the last block of the buffer
                    
                    pushToLastBlock(buffer[current_j_block].entries[jj%MAX_RECORDS_PER_BLOCK],&buffer[nmem_blocks-1],outfile,nios);
                    (*nres)++;

                    // Raise the jj counter
                    jj++;

                    // Compare the next 2 records
                    compare = fieldCompare(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],buffer[current_j_block].entries[jj%MAX_RECORDS_PER_BLOCK],field);
                    //cout << buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK].num << "-" << buffer[0].entries[jj%MAX_RECORDS_PER_BLOCK].num << "=" << compare << "  " << flush;

                }
                do{
                    pushToLastBlock(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],&buffer[nmem_blocks-1],outfile,nios);
                    // Raise the i counter
                    i++;

                    // Read the next block of the big file, if the current block is finished
                    if (i % MAX_RECORDS_PER_BLOCK == 0){
                        fread(&buffer[nmem_blocks-2],1,sizeof(block_t),big_sorted_file);
                        (*nios)++;
                        if (i>=big_size*MAX_RECORDS_PER_BLOCK){
                            break;
                        }

                    }
                    // Compare the next 2 records
                    compare = fieldCompare(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],buffer[0].entries[j%MAX_RECORDS_PER_BLOCK],field);
                    // Raise the iterations counter
                    current_iterations++;
                }
                while (compare==0);
            }

            // If the current_j_block is not 0, then it means that the last read block is in the current j block position in the buffer
            // Swap it with the 0 one, in order to continue the algorithm and not read the same blocks again
            if (current_j_block > 0){
                    buffer[0] = buffer[current_j_block];
            }

            // Set j equal to jj, because all the previous records were compared to all the i records
            j = jj;


            // Again check if the current block has ended, in order to read another
            if (j % MAX_RECORDS_PER_BLOCK == 0){
                fseek(small_sorted_file,sizeof(int) + (j/MAX_RECORDS_PER_BLOCK) * sizeof(block_t), SEEK_SET);
                fread(&buffer[0],1,sizeof(block_t),small_sorted_file);
                (*nios)++;

            }
        }
        else // If the record fields are not equal...
        {
            // If the i record is higher, raise the j record
            if (compare > 0){
                j++;

                // Also check if the block has finished, to read another one...
                if (j % MAX_RECORDS_PER_BLOCK == 0){
                    fseek(small_sorted_file,sizeof(int) + (j/MAX_RECORDS_PER_BLOCK) * sizeof(block_t), SEEK_SET);
                    fread(&buffer[0],1,sizeof(block_t),small_sorted_file);
                    (*nios)++;
                }
            }

            // If the j record is higher, raise the i record
            else{
                i++;
                // Also check if the block has finished, to read another one...
                if (i % MAX_RECORDS_PER_BLOCK == 0){
                    fread(&buffer[nmem_blocks-2],1,sizeof(block_t),big_sorted_file);
                    (*nios)++;
                }
            }

        }

    }

    // Close the files
    fclose(small_sorted_file);
    fclose(big_sorted_file);

}


void HashJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios){
    char small_file_name[100];
    char big_file_name[100];
    int small_size,big_size,first_size,second_size;
    unsigned int *nsorted_segs = new unsigned int;
    unsigned int *npasses = new unsigned int;
    char filename[100];
    buffer_record_size = (nmem_blocks-2)* MAX_RECORDS_PER_BLOCK;
    // Initialize counters
    
    // Delete previous output file, if there is one
    FILE *del = fopen(outfile,"w");
    fwrite(NULL,0,0,del);
    fclose(del);
    
    FILE *input1 = fopen(infile1, "rb");
    FILE *input2 = fopen(infile2, "rb");
    FILE *hashfile, *probefile, *temp, *output;

    //Obtain the file size
    fseek(input1, 0L, SEEK_END);
    first_size = ftell(input1)/sizeof(block_t);
    
    fseek(input2, 0L, SEEK_END);
    second_size = ftell(input2)/sizeof(block_t);
    //fclose(input1);
    //fclose(input2);
    
    // Declare smallfile=hashfile and bigfile=probefile
    if(first_size < second_size){
        hashfile = fopen(infile1,"r");
        probefile = fopen(infile2,"r");
        strcpy(big_file_name,infile2);
        big_size = second_size;
        strcpy(small_file_name,infile1);
        small_size = first_size;
    }
    else{
        hashfile = fopen(infile2,"r");
        probefile = fopen(infile1,"r");
        strcpy(big_file_name,infile1);
        big_size = first_size;
        strcpy(small_file_name,infile2);
        small_size = second_size;
    }

    cout << "Small file is " << small_file_name << " with " << small_size << " blocks" << endl;
    cout << "Big file is " << big_file_name << " with " << big_size << " blocks" << endl;
    
    output = fopen (outfile,"w");
    
    int pos;
    
    //Read first block from hashfile
    fread(&buffer[nmem_blocks-2],1,sizeof(block_t),hashfile);
    (*nios)++;
    int j=0;
    while (j < small_size*MAX_RECORDS_PER_BLOCK){
        //find position of record
        pos = hFunction(buffer[nmem_blocks-2].entries[j%MAX_RECORDS_PER_BLOCK],field);
        if (!buffer[pos/MAX_RECORDS_PER_BLOCK].entries[pos%MAX_RECORDS_PER_BLOCK].valid){
        //bucket is empty,add the matching record 
            buffer[pos/MAX_RECORDS_PER_BLOCK].entries[pos%MAX_RECORDS_PER_BLOCK] = buffer[nmem_blocks-2].entries[j%MAX_RECORDS_PER_BLOCK];
        }
        else{//bucket is full, add record to corresponding file
            sprintf(filename, "tempFiles/file%d", pos);
            temp  = fopen(filename, "a");
            fwrite(&buffer[nmem_blocks-2].entries[j%MAX_RECORDS_PER_BLOCK], 1, sizeof(record_t), temp);
            fclose(temp);
            (*nios)++;
        }
        j++;
        //if the block has finished, to read another one...
        if (j% MAX_RECORDS_PER_BLOCK==0){
            fread(&buffer[nmem_blocks-2],1,sizeof(block_t),hashfile);
            (*nios)++;
        }
    }
    fclose(hashfile);
    
    //Read first block from probefile and check with hash table
    fread(&buffer[nmem_blocks-2], 1, sizeof(block_t), probefile);
    (*nios)++;
    int i=0;
    while(i< big_size*MAX_RECORDS_PER_BLOCK){
        //for every record in the block get their result with the hash function
        pos = hFunction(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],field);
        //get the matching bucket
        if (buffer[pos/MAX_RECORDS_PER_BLOCK].entries[pos%MAX_RECORDS_PER_BLOCK].valid){//bucket is not empty,check record
            //if record in bucket matches,set write to true to write record to output block
            if (fieldCompare(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],buffer[pos/MAX_RECORDS_PER_BLOCK].entries[pos%MAX_RECORDS_PER_BLOCK],field)==0){
                pushToLastBlock(buffer[pos/MAX_RECORDS_PER_BLOCK].entries[pos%MAX_RECORDS_PER_BLOCK],&buffer[nmem_blocks-1],outfile,nios);
                pushToLastBlock(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],&buffer[nmem_blocks-1],outfile,nios);
                (*nres)++;
            }
            else{ //if the records do not match,there may be another record matching in the file for that bucket
            //check if there is a file for that bucket
                record_t temp_record;
                sprintf(filename, "tempFiles/file%d", pos);
                temp  = fopen(filename, "r");
                //read one by one the records in it and try to find a match
                while(fread(&temp_record, 1, sizeof(record_t), temp)){
                    (*nios)++;
                    //if record in bucket matches,write to output block
                    if (fieldCompare(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],temp_record,field)==0){
                        pushToLastBlock(buffer[nmem_blocks-2].entries[i%MAX_RECORDS_PER_BLOCK],&buffer[nmem_blocks-1],outfile,nios);
                        pushToLastBlock(temp_record,&buffer[nmem_blocks-1],outfile,nios);
                        (*nres)++;
                        break;
                    }
                }
                fclose(temp);
            }
        }
        i++;
        //if the block has finished, to read another one...
        if (i% MAX_RECORDS_PER_BLOCK==0){
            fread(&buffer[nmem_blocks-2],1,sizeof(block_t),probefile);
            (*nios)++;
        }
    }
    
    fclose(probefile);
    fclose(output);
}


int hFunction(record_t record,unsigned char field){
    int f = field-48;
    if (f==0 ){ //hash for recid
        return record.recid % buffer_record_size;
    }
    else if (f==1){ //hash for num
        return record.num % buffer_record_size;
    }
    else if (f==2){ //hash for string
        int sum = 0;
        int i = 0;
        while(record.str[i] != '\0'){
            sum += record.str[i];
            i++;
        }
        return sum % buffer_record_size;
    }
    else{ //hash for num and string
        int sum = 0;
        int i = 0;
        while(record.str[i] != '\0'){
            sum += record.str[i];
            i++;
        }
        return (sum+record.num) % buffer_record_size;
    }
}

    
void pushToLastBlock(record_t rec,block_t *block, char *outfile,unsigned int *nios){
    block->entries[block->nreserved] = rec;

    block->nreserved++;

    if(block->nreserved == MAX_RECORDS_PER_BLOCK){
        FILE *output = fopen(outfile , "a");
        
        for(int j=0 ; j<MAX_RECORDS_PER_BLOCK ; j++){
            fprintf(output,"Recid: %d  number: %d  str: %s \n",block->entries[j].recid,block->entries[j].num,block->entries[j].str);
            (*nios)++;
        }

        fprintf(output, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" );
        block->nreserved = 0;

        fflush(output);
        fclose(output);
    }
}

int fieldCompare(record_t rec1, record_t rec2, unsigned char field){
    int f = field - 48;

    switch (f){
        // Compare recid
        case 0:
            return rec1.recid - rec2.recid;

        // Compare number
        case 1:
            return rec1.num - rec2.num;

        // Compare string
        case 2:
            return strcmp(rec1.str,rec2.str);

        // Compare number & string
        case 3:
            if(rec1.num != rec2.num)
                return rec1.num - rec2.num;
            else
                return strcmp(rec1.str,rec2.str);

    }
}

// Print the results in the output file
void PrintMergeSortResults(char *file){
    FILE *input_file,*output_file;
    //segment_t *segment = new segment_t;
    block_t *block = new block_t;
    int seg_size;

    char in_bin[100];
    char out_txt[100];

    strcpy(in_bin,file);
    strcat(in_bin,".bin");

    strcpy(out_txt,file);
    strcat(out_txt,".txt");

    input_file = fopen(in_bin,"rb");

    fread(&seg_size,1,sizeof(int),input_file);

    cout << endl << "Writing the output..." << endl;

    output_file = fopen(out_txt,"w");

    do{
        for(int i=0 ; i<seg_size; i++){
            fread(block,1,sizeof(block_t),input_file);
            for(int j=0 ; j<MAX_RECORDS_PER_BLOCK ; j++){
                fprintf(output_file,"Block #%d  recid: %d  number: %d  str: %s \n",i,block->entries[j].recid,block->entries[j].num,block->entries[j].str);
            }
            fprintf(output_file, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" );
        }
        fread(&seg_size,1,sizeof(int),input_file);
    } while(!feof(input_file));

    fclose(input_file);
    fflush(output_file);
    fclose(output_file);

    cout << "Finished! Check \"" << out_txt << "\" for the results." << endl;

    delete block;
}

// Compare the field "Recid" amd returns their difference
int compareRecid (const void * a, const void * b){
    record_t *r1 = (record_t*) a;
    record_t *r2 = (record_t*) b;
    int r1recid = r1->recid;
    int r2recid = r2->recid;
    return (r1recid-r2recid);
}

// Compare the field "number" amd returns their difference
int compareNum (const void * a, const void * b){
    record_t *r1 = (record_t*) a;
    record_t *r2 = (record_t*) b;
    int r1num = r1->num;
    int r2num = r2->num;
    return (r1num-r2num);
}

// Compare the field "string" amd returns their difference
int compareStr (const void * a, const void * b){
    record_t *r1 = (record_t*) a;
    record_t *r2 = (record_t*) b;
    char* r1str = r1->str;
    char* r2str = r2->str;
    return strcmp(r1str,r2str);
}

// Compare first the field "number" and after the field "string" amd returns their difference
int compareNumStr (const void * a, const void * b){
    if(compareNum(a,b) != 0)
        return compareNum(a,b);

    return compareStr(a,b);
}

// Returns the minimum value
int min(int a, int b){
    if (a<=b)
        return a;
    return b;
}


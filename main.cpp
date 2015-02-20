//============================================================================
// Name        : InfRetProject.cpp
// Author      : John, Sitaras
// Version     :
// Copyright   : No copyright plis
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <pthread.h>
#include <sstream>
#include <fstream>
#include <string>
#include <stdlib.h>
#include <unordered_map>
#include <tgmath.h>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <sys/time.h>  
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

using namespace std;

ifstream file; // file to read Data and Queries
ofstream outFile; // file to write results
ifstream query_file; // file to read Queries from

int documentsNumber = 0; // number of documents ;)
int queriesNumber = 0; // number of queries ;)

// mutexes to be used to lock critical sexions
pthread_mutex_t mutexInverIndex;
pthread_mutex_t mutexReadFile;
pthread_mutex_t mutexReadFile2;
pthread_mutex_t mutexReadFile3;
pthread_mutex_t mutexQuery;


unordered_map<int, double> doc_meter; // a map to hold each document's norm


// data for each word in index
class Numbers {
    
    public:
        
        int exists; // number of times word appears in a document
        float weight; // weight of word in document
        vector<int> positions; // position of word in document for a trick
        int vec_pos = 0;
        
        Numbers() {
            exists = 0;
            weight = 0.0;
        }
        
        void setExists(int exists) {
            this->exists = this->exists + exists;
        }
        
        void setWeight(float weight) {
            this->weight = weight;
        }
        
        int getExists() {
            return exists;
        }
        
        float getWeight() {
            return weight;
        }
    
};


// represents an Inverted Index position - corresponds to a word
class Thing {
    
    public:
        
        int crowd; // number of documents including this word
        unordered_map <int, Numbers> list; // document ids that include this word
        unordered_map <int, Numbers> query_list; // query ids that include this word

        Thing() {
            crowd = 0;
        }
        
        void incrementCrowd() {
            crowd++;
        }
    
};


unordered_map <string, Thing> InvertedIndex; // the Inverted Index
unordered_map <int, int> term_freq; // a map holding number of words for each document/query
unordered_map <string, int> stopwords; // a map containing stopwords


//dat inverted Index
void invertedIndex() {

}

// auxiliary fuction to print the Inverted Index(not used)
void printMap() {
    
    for ( auto it = InvertedIndex.begin(); it != InvertedIndex.end(); ++it ) {
        cout << it->first << " " << it->second.crowd << " : ";

        for ( auto ti = InvertedIndex[it->first].list.begin(); ti != InvertedIndex[it->first].list.end(); ++ti ) {
            cout << ti->first << " ";
        }
        cout << endl;
    }
    cout << endl;
    
}

// auxiliary fuction to print the queries in the Inverted Index(not used)
void printQueryMap() {
    
    for ( auto it = InvertedIndex.begin(); it != InvertedIndex.end(); ++it ) {
        cout << it->first << " : ";
        
        for ( auto ti = InvertedIndex[it->first].query_list.begin(); ti != InvertedIndex[it->first].query_list.end(); ++ti ) {
            cout << ti->first << " ";
        }
        cout << endl;
    }
    cout << endl;
    
}


// checks words to remove punctuation and such
string checkWord(string word) {

    // remove punctuation
    char ch = word.back();
    if (ispunct(ch)) {
        word = word.substr(0, word.size()-1);
    }
    
    // toLower case for all words in index
    transform(word.begin(), word.end(), word.begin(), ::tolower);
    
    // remove stopwords
    if (stopwords.find(word) != stopwords.end()) {
        word = "";
    }

    return word;
    
}


// function to be executed by threads to build Inverted Index
void *call_from_thread(void *a) {

    string str;
    
    while (true)
    {
        // read a line/document from input file
        pthread_mutex_lock (&mutexReadFile);
        getline(file, str); // lock section to read from file
        pthread_mutex_unlock (&mutexReadFile);
        
        // if the end of file has been reached then stop reading
        if (file.eof()) {
            break;
        }
        
        string wordId;
        
        // get document's id
        int id = atoi(str.c_str());

        // convert id to string to check length
        wordId = to_string(id);
        
        // remove id and keep document
        str = str.substr(wordId.length(), str.length());

        
        istringstream iss(str);
        string word;

        
        // initialize structure
        if(term_freq.find(id) != term_freq.end())
        {
            term_freq[id] = 0;
        }

        
        // an auxialary mini inverted index for this document
        unordered_map <string, Numbers> map;
        
        int count = 0;
        int position = 1;

        
        // read every word in document
        while(iss >> word)
        {
            
            // remove punctuation
            word = checkWord(word);
            if (word == "") {
                continue;
            }

            count++; // count words
            
            
            // set mini index's stuff
            map[word].setExists(1);
            map[word].positions.resize(map[word].positions.size() + 1);
            map[word].positions.at(map[word].vec_pos) = position;
            map[word].vec_pos ++;
            position++;

        } 
        
        
        // lock critical section to edit Inverted Index and write mini index to it
        pthread_mutex_lock (&mutexInverIndex);

        // for every word in mini index
        for ( auto it = map.begin(); it != map.end(); ++it ) {

            word = it->first;
            if (InvertedIndex.find(word) != InvertedIndex.end()) // if word already exists in Inverted Index
            {

                if (InvertedIndex[word].list.find(id) == InvertedIndex[word].list.end()) {
                    
                    InvertedIndex[word].list[id] = map[word]; // add word info to the respective document in the word's list in the Inverted Index
                }

            }
            else { // if word does not exist in Inverted Index
                
                // A thing - information for each word in Inverted Index
                Thing thing;    
                InvertedIndex[word] = thing;

                InvertedIndex[word].list[id] = map[word]; // add word info to the respective document in the word's list in the Inverted Index

            }

            // increment a word's crowd
            InvertedIndex[word].incrementCrowd();
            
            // set number of times a word appears in a document in the Inverted Index
            InvertedIndex[word].list[id].setExists(it->second.getExists());

        }

        // number of words in a document
        term_freq[id] = count;
        
        // release the lock from critical section for other threads to access and edit the Inverted Index
        pthread_mutex_unlock (&mutexInverIndex);


    }
    

    return NULL;
} 


// calculate weight for a word in Inverted Index using TF x IDF
double compute_word_weight(string word, int id)
{
        
    double td;

    double idf;

    double weight;


    td = (double)InvertedIndex[word].list[id].getExists() / (double)term_freq[id];

    idf = log((double)documentsNumber / (double)InvertedIndex[word].crowd);

    weight = td * idf;

    // set a word's weight in a specific document
    InvertedIndex[word].list[id].setWeight(weight);
    

    return weight;
       
}


// calculate a document's square of a norm
void compute_desired_weights(){
     
    for(auto it = InvertedIndex.begin(); it != InvertedIndex.end(); ++it)
    {
        string word = it->first;

        for(auto ti = InvertedIndex[word].list.begin(); ti != InvertedIndex[word].list.end(); ti++)
        {
            int doc_id = ti->first;

            double word_weight = compute_word_weight(word, doc_id);
            if(doc_meter.find(doc_id) != doc_meter.end())
            {
                doc_meter[doc_id] += (word_weight * word_weight); 
            }
            else
            {  
                doc_meter[doc_id] = (word_weight * word_weight); 
            }

        }
        
    }
     
}



// compute weight for a word in a query using TF x IDF
double compute_word_weight_query(string word, int id)
{
    if (InvertedIndex[word].query_list[id].getExists() != 0) {
        double td;

        double idf;

        double weight;

        td = (double)InvertedIndex[word].query_list[id].getExists() / (double)term_freq[id];

        idf = log((double)documentsNumber / (double)InvertedIndex[word].crowd);

        weight = td * idf;

        return weight;
    }
    else {
        return 0.0;
    }
         
}



// perform a union operation between two words' document lists
unordered_map<int, int> compute_union(unordered_map<int, int> m1, unordered_map<int, Numbers> m2)
{

    unordered_map<int, int> unimap;

    for (auto it = m1.begin(); it != m1.end(); ++it) {
        unimap[it->first] = 1;
    }

    for (auto it = m2.begin(); it != m2.end(); ++it) {
        unimap[it->first] = 1;
    }

    return unimap;
    
}



// calculates cosine distance between a document and query
double cosDist(int v1, int v2, vector <string> query_words) {
    
    double sum = 0, metr1 = doc_meter[v1], metr2 = doc_meter[v2];
    int length = query_words.size();

    // for every word in a query
    for (int i = 0; i < query_words.size(); i++ )
    {   
        
        string word = query_words.at(i);
        
        // if document with id v1 contains word
        if (InvertedIndex[word].list.find(v1) != InvertedIndex[word].list.end()) {
            
            // calculate dot product
            sum = sum + (InvertedIndex[word].list[v1].getWeight() * InvertedIndex[word].query_list[v2].getWeight());
            
        }

    }
        

    
    // checks if some words of a query are found adjacent in a document
    // in that case add some extra points to their similarity
    
    double points = 0;

    if(length > 1)
    {

        int w = 2;
        
        int i = 0;
        int j = 0;
        int w1 = 0;
        int w2 = 1;
        
        int pos;

        while(w2 < length && w1 != w2)
        {
            string word1 = query_words.at(w1);
            string word2 = query_words.at(w2);

            while(InvertedIndex[word1].list.find(v1) == InvertedIndex[word1].list.end() && w1 < length)
            {

                w1++;
                i = 0;
            }
            
            while(InvertedIndex[word2].list.find(v1) == InvertedIndex[word2].list.end() && w2 < length)
            {

                w2++;
                j = 0;
            }
            
            if(w2 >= length || w1 >= length || w1 == w2)
            {
                break;
            }

            pos = InvertedIndex[query_words.at(w1)].list[v1].positions.at(i); 

            if(pos == InvertedIndex[query_words.at(w2)].list[v1].positions.at(j) - 1)
            {
                
                w1++;
                w2++;
                
                i = j;
                j = 0;
                points += 0.2;
            }
            else if(pos < InvertedIndex[query_words.at(w2)].list[v1].positions.at(j) - 1)
            {

                i++;
                if(i == InvertedIndex[query_words.at(w1)].list[v1].positions.size())
                {
                    w1++;
                    w2++;
                    i = 0;
                    j = 0;
                }
                

            }
            else
            {

                j++;
                if(j == InvertedIndex[query_words.at(w2)].list[v1].positions.size())
                {
                    w1++;
                    w2++;
                    j = 0;
                    i = 0;
                }
            }
        }
        
    }
    
    
        
    double distance = 0;

    if(metr1 != 0 && metr2 != 0)
    {
        distance = sum / (sqrt (metr1) * sqrt (metr2));
    }


    return distance + points;


}


// add query ids to each word's query list in the Inverted Index
void *call_from_thread_query(void *a) {
    
    string str;
    
    
    while (true)
    {
        
        // lock file to read
        pthread_mutex_lock (&mutexReadFile2);
        getline(query_file, str); // read query from query file
        pthread_mutex_unlock (&mutexReadFile2);
        
        
        if (query_file.eof()) {

            break;
        }
        

        string idString;
        string topkString;

        // get query's id
        int id = atoi(str.c_str());
        
        // convert id to string to check length
        idString = to_string(id);

        str = str.substr(idString.length() + 1, str.length());

        // get query's topk
        int topk = atoi(str.c_str());
        
        // convert topk to string to check length
        topkString = to_string(topk);

        // get query's text
        str = str.substr(topkString.length() + 1, str.length());
      
        
        istringstream iss(str);
        string word;

        // determine an overall id for the query to be inserted into the Inverted Index
        int column_id = documentsNumber + id;


        if(term_freq.find(column_id) != term_freq.end())
        {
          term_freq[column_id] = 0;
        }

        
        int wordcount = 0;

        
        // for each word in the query
        while(iss >> word)
        {

            if (InvertedIndex.find(word) != InvertedIndex.end()) {
                
                // lock this critical section to edit the Inverted Index
                pthread_mutex_lock (&mutexQuery);
                
                // add query's determined id to the word's list of query in the Inverted Index
                InvertedIndex[word].query_list[column_id].setExists(1);
                
                // unlock critical section
                pthread_mutex_unlock (&mutexQuery);   
                
                // count number of words in this query
                term_freq[column_id] ++; //apo edw evgala to mutex, gt kathe query grafei sto diko tou xwro opote ante geia, to exei pei kai roidis oti ginetai
                
                wordcount ++; // count number of words in this query
            }
  
        }
    

        
        // Computing weight of every term in the query
        istringstream idd(str);
        
        // map to hold this query's weights of words
        unordered_map <string, double> weight_map;

        
        // for each word
        while(idd >> word)
        {

            if (InvertedIndex.find(word) != InvertedIndex.end()) {

                // add weight to mini map
                weight_map[word] = compute_word_weight_query(word, column_id);
                
            }

        }


        
    
        //write query's word weights to index
        for ( auto it = weight_map.begin(); it != weight_map.end(); ++it ) {
            
            word = it->first;
            InvertedIndex[word].query_list[column_id].setWeight(it->second);
            
            
            // add weight to document's norm
            if(doc_meter.find(column_id) != doc_meter.end())
            {
                doc_meter[column_id] += (it->second * it->second); 
            }
            else
            {  
                doc_meter[column_id] = (it->second * it->second); 
            }

        }
    }



    return NULL;
}



// computing similarity between dem queries and documents
void *call_from_thread_query_second_half(void *a) {
    
    if (!outFile.is_open()) {
        outFile.open("out.txt", ios::app);
    }
    
    
    string str;

    
    while (true)
    {
 
        // lock this section to read from query file
        pthread_mutex_lock (&mutexReadFile3);
        getline(query_file, str); // read a line/query from query file
        pthread_mutex_unlock (&mutexReadFile3);
        
        
        if (query_file.eof()) {
            break;
        }
        
        
        string idString;
        string topkString;

        
        // get query's id
        int id = atoi(str.c_str());
        
        // convert id to string to check length
        idString = to_string(id);

        str = str.substr(idString.length() + 1, str.length());

        // get query's top-k
        int topk = atoi(str.c_str());
        
        // convert topk to string to check length
        topkString = to_string(topk);

        // get query's text
        str = str.substr(topkString.length() + 1, str.length());
        
        
        // determine an overall id for the query to be inserted into the Inverted Index
        int column_id = documentsNumber + id;

        
        Numbers *topkArray = new Numbers[topk]; // holds the top-k similarities


        string word2;


        int wordcount = 0;
        istringstream iss(str);
        string word;
        
        while(iss >> word)
        {
            wordcount ++; // count words in query
        }



        vector <string> query_words; // holds a query's unique words that exist in the Inverted Index
        
        int while_count = 0;
        int temp_wordcount = wordcount;
        
        istringstream iff(str);
        iff >> word;

        
        // stores all documents that contain a query's words, evolved from univector at level 3
        unordered_map <int, int> unimap;

        
        while (InvertedIndex.find(word) == InvertedIndex.end() && while_count < temp_wordcount) {
            iff >> word;       
            temp_wordcount --;
        }

        
        // adds documents that contain the first word of the query
        for ( auto it = InvertedIndex[word].list.begin(); it != InvertedIndex[word].list.end(); ++it ) {
            unimap[it->first] = 1;
        }

        
        query_words.resize(1);
        query_words.at(0) = word;

        
        // gather all document ids that contain this query's words
        if(wordcount > 1)
        {
            for(int count = 0; count < wordcount - 1; count++)
            {
                iff >> word2;

                if (InvertedIndex.find(word2) != InvertedIndex.end()) {

                    if(find(query_words.begin(), query_words.end(), word2) == query_words.end())
                    {
                        query_words.resize(query_words.size() + 1);
                        query_words.at(query_words.size() - 1) = word2;

                    }
                    
                    // update unimap for every word in query
                    unimap = compute_union(unimap, InvertedIndex[word2].list);


                }
            }
        }
    

        // initialize topkArray structure
        int topK_counterino = 0;
        for (int i = 0; i < topk; i++) {
            topkArray[i].weight = 0;
            topkArray[i].exists = -1;
        }

        
        // for every document in unimap(that contains this query's words)
        // compute cosine similarity between document and query
        for ( auto it = unimap.begin(); it != unimap.end(); ++it ) {
            int id = it->first;
            if (topK_counterino == topk) {

                    float similarity = (float)cosDist(id, column_id, query_words);

                    // if computed similarity is greater than the ones in topkAarray, add it to the array
                    if (similarity > topkArray[topk-1].weight) {

                        topkArray[topk-1].exists = id;
                        topkArray[topk-1].weight = similarity;

                        sort(topkArray, topkArray + topk,
                            [](Numbers const & a, Numbers const & b) -> bool
                            { return a.weight > b.weight; } 
                        );

                    }
            }
            else {
                    topkArray[topK_counterino].exists = id;
                    topkArray[topK_counterino].weight = (float)cosDist(id, column_id, query_words);
                    if (topK_counterino == topk-1) {
                        
                        // if array is full, sort array
                        sort(topkArray, topkArray + topk,
                            [](Numbers const & a, Numbers const & b) -> bool
                            { return a.weight > b.weight; } 
                        );
                    }
                    topK_counterino++;
            }
        }

        
        // sort topkArray to get topK documents with max similarity
        sort(topkArray, topkArray + topk,
                            [](Numbers const & a, Numbers const & b) -> bool
                            { return a.weight > b.weight; } 
                        );
                        
                        
        // write result in out file
        outFile << id << ". ";
        //cout << "---------------- PRINTING FOR QUERY " << id << " (topk=" << topk << ")" << "----------------------------" << endl;
        for (int i = 0; i < topK_counterino; i++) {
            //cout << "Doc " << topkArray[i].exists << " with cosDist " << topkArray[i].weight << endl;
            outFile << topkArray[i].exists;// << ", ";
            if (i < topK_counterino-1) {
                outFile << ", ";
            }
        }
        outFile << "\n";
        //cout << "---------------- ENDOF PRINTING FOR THIS QUERY ----------------------------" << endl;


    }
    
 
}



// read documents from input file and build Inverted Index by summoning tn threads
void readFile(char *filename, int tn) {
    
    
    file.open(filename); // open input file
    
    // if file exists
    if (file) {
    
        string str;

        pthread_t thread[tn]; // an array holding threads

        // read input file's first line - the number of documents
        getline(file, str);
        int docNumber = atoi(str.c_str());
        
        documentsNumber = docNumber;


        // begin timer for Inverted Index creation
        struct timeval tim;
        auto t_start = chrono::high_resolution_clock::now();
        gettimeofday(&tim, NULL);  
        double t1=tim.tv_sec+(tim.tv_usec/1000000.0);  
        
        
        // start threads
        for (int i = 0; i < tn; i++) {
            pthread_create(&thread[i], NULL, call_from_thread, NULL);
        }
        

        // join threads
        for(int i = 0; i < tn; i++)
        {
            pthread_join(thread[i], NULL);
        }
        
        
        // close file
        file.close();


        // stop timer for Inverted Index creation
        auto t_end = chrono::high_resolution_clock::now();
        gettimeofday(&tim, NULL);  
        double t2=tim.tv_sec+(tim.tv_usec/1000000.0);
        cout << "INDEX CREATION: " << (t2-t1)*1000 << " ms\n";
    
    }
    else { // no comments...
        
        cout << "Input file does not exist!" << endl;
        exit(0);
        
    }
  
} 


// read queries from query file and execute them by summoning more tn threads
void readQueryFile(char *filename, int tn) {

    // open query file
    query_file.open(filename);
    
    // if query file exists
    if (query_file) {
    
        string str;

        pthread_t thread[tn]; // an array holding threads

        
        // read input file's first line - the number of documents
        getline(query_file, str);
        int docNumber = atoi(str.c_str());
        
        queriesNumber = docNumber;

        
        // begin timer for queries execution
        struct timeval tim;
        auto t_start = chrono::high_resolution_clock::now();
        gettimeofday(&tim, NULL);  
        double t1=tim.tv_sec+(tim.tv_usec/1000000.0);  


        // start threads to compute weights for every word in queries
        for (int i = 0; i < tn; i++) 
        {
            pthread_create(&thread[i], NULL, call_from_thread_query, NULL);
        }


        // join threads
        for(int i = 0; i < tn; i++)
        {
            pthread_join(thread[i], NULL);
        }
        
        
        // close query file
        query_file.close();

        
        // computes weights for every word in every document
        compute_desired_weights();

        
        
        pthread_t thread_plis[tn]; // another array holding threads
        
        
        // reopen query file
        query_file.open(filename);
        
        
        getline(query_file, str);
        
        
        // start threads to execute queries
        for (int i = 0; i < tn; i++) 
        {
            pthread_create(&thread_plis[i], NULL, call_from_thread_query_second_half, NULL);
        }
        
        
        // join threads
        for(int i = 0; i < tn; i++)
        {
            pthread_join(thread_plis[i], NULL);
        }

        
        // just a line in out file
        outFile << endl;
        outFile << "----------------------------------------------------------------------------------------------" << endl;
        outFile << endl;

        
        // stop timer for query execution
        auto t_end = chrono::high_resolution_clock::now();
        gettimeofday(&tim, NULL);  
        double t2=tim.tv_sec+(tim.tv_usec/1000000.0);
        cout << "QUERY EXECUTION: " << (t2-t1)*1000 << " ms\n";
    
    }
    else { // no comments...
        
        cout << "Query file does not exist!" << endl;
        exit(0);
        
    }

}

// creates a map containing stopwords
void createStopwords() {
    
    ifstream file("stopwords.txt");
    string word;

    while (getline(file, word)) {
        
        stopwords[word] = 0;
        
    }

}



int main() {
    
    
    int threads_num;
    char c, input[20], queries[20];
    
    cout << "Please specify input file name(max chars: 20, don't forget to include '.txt' extension!!!): ";
    cin.getline(input, 20);
    
    cout << "Please specify query file name(max chars: 20, don't forget to include '.txt' extension!!!): ";
    cin.getline(queries, 20);
    
    cout << "Please give number of threads: ";
    cin >> threads_num;
    
    cout << "Do you want to remove stopwords?(y/n): ";
    cin >> c;
    
    if (c == 'y') {
        createStopwords();
    }
    else if (c == 'n') {
        
    }
    else {
        cout << "You were supposed to insert y or n... How hard is that to understand?" << endl;
        exit(0);
    }
        
    
    // start timer
    struct timeval tim;
    auto t_start = chrono::high_resolution_clock::now();
    gettimeofday(&tim, NULL);  
    double t1=tim.tv_sec+(tim.tv_usec/1000000.0);
    
    
    // initialize mutexes
    pthread_mutex_init(&mutexInverIndex, NULL);
    pthread_mutex_init(&mutexReadFile, NULL);
    pthread_mutex_init(&mutexReadFile2, NULL);
    pthread_mutex_init(&mutexReadFile3, NULL);
    pthread_mutex_init(&mutexQuery, NULL);

    
    // start building Inverted Index
    readFile(input, threads_num);
    
    
    // start executing queries
    readQueryFile(queries, threads_num);

    
    //printMap();
    
    
    // stop timer
    auto t_end = chrono::high_resolution_clock::now();
    gettimeofday(&tim, NULL);  
    double t2=tim.tv_sec+(tim.tv_usec/1000000.0);
    
    
    // seek and destroy mutexes
    pthread_mutex_destroy(&mutexInverIndex);
    pthread_mutex_destroy(&mutexReadFile);
    pthread_mutex_destroy(&mutexReadFile2);
    pthread_mutex_destroy(&mutexReadFile3);
    pthread_mutex_destroy(&mutexQuery);
    
    
    // print durations
    cout << "Wall clock time passed: "
              << std::chrono::duration<double, std::milli>(t_end-t_start).count()
              << " ms\n";
    cout << "Pure time passed: " << (t2-t1)*1000 << " ms\n";

    
    // Man with hairsssss........
    cout << "Man with hairsssss" << endl;
     

    
    return 0;
    
}

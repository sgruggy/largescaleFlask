import pymongo
from datetime import datetime
import tokenizeMsg
import re
import math

#connecting to database, can ignore
connection = pymongo.MongoClient("ds127644.mlab.com", 27644)
db = connection["largescale"]
db.authenticate("largescale", "root56")

#database names
message_col = db["Message"]
word_col = db["Word"]

#functions
'''
serializes the message, and updates all the words in the word index for that message
Example: the word "this" appears in document id 0, positions 2, 11, 21
The resulting data in the collection would look like:
{
    "word": "this",
    "indexed": [[0, [2, 11, 21]]]
}

If anther message with id 1 comes in, and "this" appears in positions 4, 15
The resulting updated data would look like:
{
    "word": "this",
    "indexed": [[0, [2, 11, 21]], [1, [4, 15]]]
}

@param message: JSON or dict data type
@param word_col: the Word collection in mlab
@return return: None
'''
def serialize_message_to_word(message):
    m_id = message["message_id"]
    word_table = tokenizeMsg.tokenizeMsg(message["content"])
    '''
    With the resulting table, update the respective words in the collection with
    the new indexes
    '''
    for word in word_table.keys():
        if word_col.count_documents({"word": word}) > 0:
            to_modify = word_col.find_one({"word": word})
            index_info = to_modify["indexed"]
            index_info.append([m_id, word_table[word]])

            word_col.update_one(
                {"word": word}, 
                {
                    "$set": {
                        "indexed": index_info
                            }
                })
        else:
            to_insert = {
                "word": word,
                "indexed": [[m_id, word_table[word]]]
            }
            word_col.insert_one(to_insert)


def insert_message(request):
    message = {
        "message_id": message_col.count_documents({}),
        "username":request.username,
        "content":request.message,
        "post_date":request.datePosted
    }
    message_col.insert_one(message)
    return message


'''
This function will return the message ids of the scalica messages from a query

@param query: String, search query
@param word_col: the Word collection in mlab
@return: set of Message ids
'''
def search_get_message_ids(query):
    ids = set()
    cursor = word_col.find({
        "word" : {
            "$in" : tokenizeMsg.tokenizeSearch(query)
            }
        })
    for word in cursor:
        indexes = word["indexed"]
        for index in indexes:
            ids.add(index[0])
    
    return ids

'''
For Eddie's scoring method, returns a list of messages.
For reference, the Message object looks like this:
Message{
    "message_id": int
    "content": String
    "username": String
    "date_posted": Datetime
}

@param ids: set of ids
@param message_col: Message collection on mlab
@return: list of messages
'''
def search_get_messages(query):
    ids = search_get_message_ids(query)
    cursor = message_col.find({
        "message_id" : {
            "$in" : list(ids)
        }
    })
    message_list = []
    for message in cursor:
        message_list.append(message)

    return message_list


########################## SCORING STUFF ########################
'''
Function to calcuate the tf_idf of one word for a specific message. 

@param mess_in - message
@param word_in - word 
@param mess_count_in - total # of messages in database
@param word_doc_count_in - # of documents that contain the word
@return - float tf_idf score. 
'''
def get_tf_idf(mess_in, word_in, mess_count_in, word_doc_count_in):
    # Tokenize message and get length. 
    mess_dict = tokenizeMsg.tokenizeMsg(mess_in)
    mess_len = len(mess_in.split(' '))
    # Calculate tf = term frequency = (# times word occurs in message) / (# words in message).
    tf =0
    if word_in in mess_dict.keys():
        tf = len(mess_dict[word_in]) / mess_len 
    # Calculate idf = inverse document frequency = log(# messages / # messages containing word).
    # mess_count -> from message_sort()
    # word_doc_count -> from message_sort()
    idf = math.log(mess_count_in / word_doc_count_in)
    # Calculate td_idf : (tf * idf)
    tf_idf = tf * idf
    return tf_idf


'''
Function to create a list of messages sorted (Greatest to Least by TF_IDF score)

@param mess_list_in - list of messages that contain at least one of the words from the query.
@param query_in - String that contains the user inputted query. 
@return - list of tuples (message, tf_idf score) -> sorted greatest to least by tf_idf score. 
'''
def sorted_messages(mess_list_in, query_in):
    mess_score_list = [] # List with (message_id, score)
    mess_count = message_col.count_documents({}) # Count of all messages. 
    
    cursor = word_col.find({
            "word" : {
                "$in" : tokenizeMsg.tokenizeSearch(query_in)
                }
            })
    
    word_table = []
    for word in cursor:
        word_table.append(word)
    
    # Loop through all messages.
    for message in mess_list_in:
        # For each word in the query
        mess_score = 0
        for word in word_table:
            word_doc_count = len(word["indexed"]) # Count of all words
            mess_score += get_tf_idf(message["content"], word["word"], mess_count, word_doc_count)
        
        # Add to mess_score_list
        mess_score_list.append((message, mess_score))
    
    # Sort mess_score_list
    mess_score_list.sort(key=lambda x:x[1], reverse=True)

    return mess_score_list
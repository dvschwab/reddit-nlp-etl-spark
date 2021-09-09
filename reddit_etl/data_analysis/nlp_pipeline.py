### NLP Pipeline Functions

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

def tokenize(nlp_df):
    
    nlp_tokenizer = Tokenizer(inputCol="body", outputCol="post_words")
    tokenized_df = nlp_tokenizer.transform(nlp_df)
    return tokenized_df

def remove_stopwords(tokenized_df):

    remover = StopWordsRemover(inputCol='post_words', outputCol='post_filtered')
    remover.loadDefaultStopWords('english')
    filtered_df = remover.transform(tokenized_df)
    return filtered_df

def hasher(hashable_df):

# Number of Features is default (262,144)

    hasher = HashingTF(inputCol='post_filtered', outputCol='post_hashed')
    hashed_df = hasher.transform(hashable_df)
    return hashed_df

def tfidf_calc(hashed_df):

    tfidf = IDF(inputCol='post_hashed', outputCol='post_tfidf')
    tfidfModel = tfidf.fit(hashed_df)
    tfidf_df = tfidfModel.transform(hashed_df)
    return tfidf_df
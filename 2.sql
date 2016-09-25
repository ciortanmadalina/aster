-- https://10.128.24.11/appserver/portal

drop table if exists npath_mc255050;
create table npath_mc255050 distribute by replication as
SELECT d_path as path, count(*) as cnt 
FROM nPath (
    ON  load.monthly_perform 
    PARTITION BY loan_seq_num
    ORDER BY mo_rpt_period ASC
    MODE (NONOVERLAPPING)
    PATTERN ('d_code*')
    SYMBOLS (curr_delinqency_cd <> '0' AS d_code)
    RESULT (
	accumulate(curr_delinqency_cd OF ANY (d_code)) AS d_path
    )
)
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 30;

select * from  npath_mc255050 limit 100;

-- ================================================================================
-- ================================================================================
-- title : dihub demo
-- version : 1.0
-- date : 21-sept-2016
-- author 1 : mark oost, data scientist
-- ================================================================================
-- ================================================================================

--create the data
drop table if exists mc255050.reviews; 
create table  mc255050.reviews distribute by hash(id) as
select * from load.amazon limit 50000;

-- ================================================================================
-- ================================================================================
-- step 1 : sentiment extraction using dictionary
-- ================================================================================
-- ================================================================================


--sentiment analysis
drop table if exists mc255050.sentiment; 
create table mc255050.sentiment distribute by hash(id) as
SELECT * FROM ExtractSentiment (
	ON mc255050.reviews
	Text_Column('totaltext')
	Model('dictionary')
	Level('document')
	Accumulate ('id', 'userid', 'productid')
) where out_polarity = '${pol}' order by id;

select * from mc255050.sentiment limit 100;

-- ================================================================================
-- ================================================================================
-- step 2 : TOP x review
-- ================================================================================
-- ================================================================================

drop table if exists mc255050.sentiment_top;
create table mc255050.sentiment_top distribute by hash(productid) as
select productid, count(*) as total from mc255050.sentiment 
group by 1 order by total desc limit 50;


insert into app_center_visualizations (json) values (
'{
     "db_table_name":"mc255050.sentiment_top",
     "vizType":"table",
     "version":"1.0",
     "where" : "",
     "title":"diagram"
}');

-- ================================================================================
-- ================================================================================
-- step 3 : creating ngrams fro TF-IDF
-- ================================================================================
-- ================================================================================

--create ngrams
drop table if exists mc255050.ngram;
create table mc255050.ngram distribute by hash(id) as
SELECT * FROM nGram (
	ON mc255050.sentiment
	Text_Column ('out_sentiment_words')
	Delimiter (' ')
	Grams ('1')
	Punctuation ('\[.,?\!0-9\]')
	Reset ('\[.,?\!\]')
	Accumulate ('id', 'userid', 'out_polarity', 'out_strength')
) ORDER BY id;

--delete some ngrams
delete from mc255050.ngram where length(ngram) < 3;
delete from mc255050.ngram where ngram = 'score:';
delete from mc255050.ngram where ngram = 'score:-';
delete from mc255050.ngram where ngram = 'positive';
delete from mc255050.ngram where ngram = 'negative';
delete from mc255050.ngram where ngram = 'total';


drop table if exists mc255050.tfidf_input1;
CREATE fact TABLE mc255050.tfidf_input1 DISTRIBUTE BY hash(term) AS
SELECT id as docid, ngram AS term, frequency AS count
FROM mc255050.ngram;

-- ================================================================================
-- ================================================================================
-- step 4 : TF-IDF
-- ================================================================================
-- ================================================================================
drop table if exists mc255050.tfidf_output1;
CREATE TABLE mc255050.tfidf_output1 DISTRIBUTE BY HASH(term) AS
	SELECT * FROM tf_idf (
		ON TF(
		ON mc255050.tfidf_input1 PARTITION BY docid) 
		AS tf PARTITION BY term
		ON (SELECT COUNT (DISTINCT docid) FROM mc255050.tfidf_input1
		) AS doccount DIMENSION
);


drop table if exists tfidf_output2_mc255050;
create table tfidf_output2_mc255050 distribute by hash(term) as
select term, sum(tf) as tf, sum(idf) as idf, sum(tf_idf) as tf_idf from mc255050.tfidf_output1
group by 1 
order by tf_idf desc limit 50;

-- ================================================================================
-- ================================================================================
-- step 5 : Creating wordlcould
-- ================================================================================
-- ================================================================================


INSERT INTO app_center_visualizations  (json) 
SELECT json FROM Visualizer (
ON "tfidf_output2_mc255050" PARTITION BY 1 
AsterFunction('tfidf') 
Title('tfidf diagram') 
VizType('wordcloud')
);


-- ================================================================================
-- ================================================================================
-- step 6 : MBA on words
-- ================================================================================
-- ================================================================================
SELECT * FROM cfilter (
ON (SELECT 1)
	PARTITION BY 1
	InputTable ('mc255050.tfidf_input1')
	OutputTable ('mc255050.cfilter_output')
	InputColumns ('term')
	JoinColumns ('docid')
	DropTable ('true')
);

-- ================================================================================
-- ================================================================================
-- step 5 : Creating sigma
-- ================================================================================
-- ================================================================================

drop table if exists cfilter_output_mc255050;
create table cfilter_output_mc255050 distribute by replication as
select * from mc255050.cfilter_output 
order by cntb desc limit 1000;



INSERT INTO app_center_visualizations  (json) 
SELECT json FROM Visualizer (
ON "cfilter_output_mc255050" PARTITION BY 1 
AsterFunction('cfilter') 
Title('words used together') 
VizType('sigma')
);
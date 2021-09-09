/* Table definitions
 Not normalized for dev
 Last changed 6/23/2021 */

use reddit_nlp;
drop table if exists onegram;

create table onegram (
	onegram_id bigint auto_increment unique primary key,
    author varchar(50) not null,
    subreddit varchar(200) not null,
    created_datetime timestamp,
    score int,
    ups int,
    controversiality int,
    gilded int,
    edited Boolean,
	stickied Boolean,
    distinguished Boolean,
    tfidf double
);
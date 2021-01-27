CREATE DATABASE instagram;

-- SELECT oid from pg_database WHERE datname = <database_name>;
-- SELECT relname, relfilenode FROM pg_class WHERE relname = <table_name>; 

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  username VARCHAR(30) NOT NULL,
  bio VARCHAR(400),
  avatar VARCHAR(200),
  phone VARCHAR(25),
  email VARCHAR(40),
  password VARCHAR(50),
  status VARCHAR(15),
  CHECK(COALESCE(phone, email) IS NOT NULL)
);

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  url VARCHAR(200) NOT NULL,
  caption VARCHAR(240),
  lat REAL CHECK(lat IS NULL OR (lat >= -90 AND lat <= 90)), 
  lng REAL CHECK(lng IS NULL OR (lng >= -180 AND lng <= 180)),
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  contents VARCHAR(240) NOT NULL,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE
);

CREATE TABLE likes (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
  comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
  CHECK(
    COALESCE((post_id)::BOOLEAN::INTEGER, 0)
    +
    COALESCE((comment_id)::BOOLEAN::INTEGER, 0)
    = 1
  ),
  UNIQUE(user_id, post_id, comment_id)
);

CREATE TABLE photo_tags (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  UNIQUE(user_id, post_id)
);

CREATE TABLE caption_tags (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  UNIQUE(user_id, post_id)
);

CREATE TABLE hashtags (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  title VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE hashtags_posts (
  id SERIAL PRIMARY KEY,
  hashtag_id INTEGER NOT NULL REFERENCES hashtags(id) ON DELETE CASCADE,
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  UNIQUE(hashtag_id, post_id)
);

CREATE TABLE followers (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  leader_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  follower_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  UNIQUE(leader_id, follower_id)
);

-- show data_directory;
-- select oid, datname from pg_database;
-- select * from pg_class;
-- select pg_size_pretty(pg_relation_size('users'));

CREATE INDEX users_username_idx ON users (username);
DROP INDEX users_username_idx;

EXPLAIN ANALYZE SELECT *
FROM users
WHERE username = 'Emil30';

select pg_size_pretty(pg_relation_size('users'));
select pg_size_pretty(pg_relation_size('users_username_idx'));

select relname, relkind
from pg_class
where relkind = 'i';

create extension pageinspect;
select * from bt_metap('users_username_idx');
select * from bt_page_items('users_username_idx', 3);

select ctid, * from users where username='Aaliyah.Hintz';

select * from pg_stats where tablename = 'users';

CREATE INDEX likes_created_at_idx ON likes (created_at);

explain select *
from likes
where created_at < '2013-01-01';

-- not using an index
explain select * 
from likes
where created_at > '2013-01-01';

select username, tags.created_at
from users
join (
  select user_id, created_at from caption_tags
  union all
  select user_id, created_at from photo_tags
) as tags
on tags.user_id = users.id
where tags.created_at < '2010-01-07';

-- Simple Common Table Expressions
with tags as (
  select user_id, created_at from caption_tags
  union all
  select user_id, created_at from photo_tags
)
select username, tags.created_at
from users
join tags on tags.user_id = users.id
where tags.created_at < '2010-01-07';

-- Recursive Common Table Expressions
with recursive countdown(val) as (
  select 3 as val -- Initail, Non-recursive query
  union
  select val - 1 from countdown where val > 1 -- Recursive query
)
select *
from countdown;


with recursive suggestions(leader_id, follower_id, depth) as (
      select leader_id, follower_id, 1 as depth
      from followers
      where follower_id = 1000
  union
      select followers.leader_id, followers.follower_id, depth + 1
      from followers
      join suggestions on suggestions.leader_id = followers.follower_id
      where depth < 3
)
select distinct users.id, users.username
from suggestions
join users on users.id = suggestions.leader_id
where depth > 1
limit 30;

-- most popular users
select username, count(*)
from users
join (
  select user_id from photo_tags
  union all
  select user_id from caption_tags
) as tags on tags.user_id = users.id
group by username
order by count(*) desc;

create view tags as (
  select id, created_at, user_id, post_id, 'photo_tag' as type from photo_tags
  union all
  select id, created_at, user_id, post_id, 'caption_tag' as type from caption_tags
);

select * from tags where type = 'caption_tag';

select username, count(*)
from users
join tags on tags.user_id = users.id
group by username
order by count(*) desc;

create view recent_posts as (
  select * from posts
  order by created_at desc
  limit 10
);

select * from recent_posts;

select *
from recent_posts
join users on users.id = recent_posts.user_id;

create or replace view recent_posts as (
  select * from posts
  order by created_at desc
  limit 15
);

drop view recent_posts;

-- Materialized View
create materialized view weekly_likes as (
  select
    date_trunc('week', COALESCE(posts.created_at, comments.created_at)) as week,
    count(posts.id) as num_likes_for_posts,
    count(comments.id) as num_likes_for_comments
  from likes
  left join posts
  on posts.id = likes.post_id
  left join comments
  on comments.id = likes.comment_id
  group by week
  order by week
) with data;

refresh materialized view weekly_likes;

-- controlling schema access
show search_path;
set search_path to test, public;

-- reset search_path
set search_path to "$user", public;
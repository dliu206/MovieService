DROP DATABASE IF EXISTS Movies;
CREATE DATABASE Movies;

USE Movies;
SET NAMES utf8 ;
SET character_set_client = utf8mb4 ;
SET SESSION group_concat_max_len = 100000;

DROP TABLE IF EXISTS Movies;

CREATE TABLE Movies (
    movie_id INTEGER NOT NULL,
    budget INTEGER NOT NULL,
    genres JSON NOT NULL,
    homepage TINYTEXT,
    keywords JSON NOT NULL,
    original_language VARCHAR(2) NOT NULL,
    original_title TINYTEXT NOT NULL,
    overview TEXT NOT NULL,
    popularity FLOAT NOT NULL,
    production_companies JSON NOT NULL,
    production_countries JSON NOT NULL,
    release_date DATE,
    revenue DOUBLE NOT NULL,
    runtime DOUBLE NOT NULL,
    spoken_languages JSON NOT NULL,
    status VARCHAR(20) NOT NULL,
    tagline TEXT NOT NULL,
    title TINYTEXT NOT NULL,
    vote_average DOUBLE NOT NULL,
    vote_count INTEGER NOT NULL,
    PRIMARY KEY (movie_id)
    );

DROP TABLE IF EXISTS Credits;
CREATE TABLE Credits (
    movie_id INTEGER NOT NULL UNIQUE,
    title TINYTEXT NOT NULL,
    cast JSON NOT NULL,
    crew JSON NOT NULL,
    PRIMARY KEY (movie_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
    );

DROP TABLE IF EXISTS Users;
CREATE TABLE Users (
    username VARCHAR(20) NOT NULL UNIQUE,
    password VARCHAR(72) NOT NULL,
    wallet_balance DOUBLE NOT NULL,
    movies JSON,
    PRIMARY KEY (username)
);

DROP TABLE IF EXISTS Genre;
CREATE TABLE Genre (
    genreId INTEGER NOT NULL UNIQUE,
    genreName VARCHAR(50) NOT NULL,
    PRIMARY KEY (genreId)
);

DROP TABLE IF EXISTS Cast;
CREATE TABLE Cast (
    castId INTEGER NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    PRIMARY KEY (castId)
);
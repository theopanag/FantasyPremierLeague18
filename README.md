# Fantasy Premier League 2017-2018 scraper 

![alt tag](https://i.ytimg.com/vi/CRk9HpmSHNM/maxresdefault.jpg)

A script to scrap the FPL website for information regarding the mini-league of your choice. It collects the info and saves it in a SQLite database so you can later on combine it with other software. I am using it to later on plot charts regarding various performance metrics of my mini-league members.

Any suggestions for improvement are always welcome!

### List of files
1. fpl_info.py : Is the main module of the software, responsible for calling the other modules and building the database
2. get_data.py : Module scraping the FPL website for the required information
3. manage_sqlite.py : Module building the database tables and the corresponding views
4. params.py : Module holding user's parameters regarding mini-league id, etc...
5. DB_Views.sql : Script with the database view definitions

##### References:
[Fantasy Premier League homepage](https://fantasy.premierleague.com/a/home)


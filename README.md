# Exploring MAV´S data using Apis.
This repo shows how to track data of one of the most important markets in Argentina: Mercado Argentino de Valores.
This market shows the different financing possibilites and instruments that Small Caps Companies in Argentina have for their porjects and their daily operations.

## What can we find here.

So in this repo you will find differents scripts that shows how to receive information from the Api soruce and to connect it to a local PostgreSQL database. 
Basically shows how to create a PostgreSQL database taking this market daily data from an Api source.
In this first step you will find a way to connect through Python an api data source to a local database. Then you will find another script that allows you to update manually, and in a daily frequency, this database. 
Finally ther's a final script(not ready yet) that automatize this update process without the need of running manually the update script everytime we want to have the latest data availabe.

## Connections to another repo.
This process it's shown in another repo where we deploy an Dash app on render using the PostgreSQL database that we create and update here. You can find the link to the repo right [here](https://dashlf.onrender.com/)


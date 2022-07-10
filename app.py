#imports 
import os
import json
from pprint import pprint
import requests

#flask
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy

#databse
import sqlalchemy as db
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy import update, delete, insert
from sqlalchemy import  MetaData, Table, Column, Integer, String, Table,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#date utils
import dateutil.parser as dt
import datetime

#config
app = Flask(__name__)
env_config = os.getenv("APP_SETTINGS", "config.DevelopmentConfig")
app.config.from_object(env_config)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
database = app.config["SQLALCHEMY_DATABASE_URI"]
engine = db.create_engine('sqlite:///database/athletes/athletes.db',{'poolclass':SingletonThreadPool})
Session = sessionmaker(bind=engine)

#instanciate db engin
connection = engine.connect()
metadata = db.MetaData()

#setup table model
nbaTeams = Table(
   'NBA_TEAMS', metadata, 
   Column('id', Integer, primary_key = True), 
   Column('abbreviation', String), 
   Column('city', String),
   Column('conference', String),
   Column('division', String),
   Column('full_name', String),
   Column('name', String),
   Column('date_founded', DateTime)
)

#create tables
metadata.create_all(engine)

# # get nba data
# nba_api_response = requests.get('https://www.balldontlie.io/api/v1/teams')
# nba_data_json = json.loads(nba_api_response.content)

# declare the data model
class NBATeam(db.Model):
    """NBA team data model."""
    __tablename__ = "NBA_TEAMS"

    id = Column('id', Integer, primary_key = True)
    abbreviation = Column('abbreviation', String, nullable = True) 
    city = Column('city', String, nullable = True)
    conference = Column('conference', String, nullable = True)
    division = Column('division', String, nullable = True)
    full_name = Column('full_name', String, nullable = True)
    name = Column('name', String, nullable = True)
    date_founded = Column('date_founded', DateTime, nullable = True)

# # insert data into the NBATeam table
# for team_entry in nba_data_json['data']:
#     team = NBATeam(
#         id = team_entry['id'],
#         abbreviation = team_entry['abbreviation'],
#         city = team_entry['city'],
#         conference = team_entry['conference'],
#         division = team_entry['division'],
#         full_name = team_entry['full_name'],
#         name = team_entry['name'])
#     try:
#         session.add(team)
#         session.commit() 
#     except Exception as e:
#         session.rollback()

# create reference to NBATeam table
nbaTeams = Table('NBA_TEAMS', metadata, autoload=True, autoload_with=engine)


def updateRow(teamData):
    """ Use data sent from front end to update a row.
    Parameters
    ----------
    teamData : NBATeam
        Object of NBATeam class containing the data to use in update transaction.

    Returns
    -------
    stmt  : sqlalchemy.sql.dml.Update
        Update statement object that can be executed by a session.
    """
    stmt = (
         update(nbaTeams)
         .where(nbaTeams.c.id == teamData.id)
         .values(
            abbreviation = teamData.abbreviation,
            city = teamData.city,
            conference = teamData.conference,
            division = teamData.division,
            full_name = teamData.full_name,
            name = teamData.name,
            date_founded = teamData.date_founded
         )
    )
    print(type(stmt))
    return stmt

def deleteRows(teamData):
    """ Use data sent from front end to delete a row.
    Parameters
    ----------
    teamData : NBATeam
        Object of NBATeam class containing the data to use in update transaction.

    Returns
    -------
    stmt  : sqlalchemy.sql.dml.Update
        Update statement object that can be executed by a session.
    """
    stmt = (
         update(nbaTeams)
         .where(nbaTeams.c.id == teamData.id)
         .values(
            abbreviation = teamData.abbreviation,
            city = teamData.city,
            conference = teamData.conference,
            division = teamData.division,
            full_name = teamData.full_name,
            name = teamData.name,
            date_founded = teamData.date_founded
         )
    )
    print(type(stmt))
    return stmt

def insertRows(team_insert_entries):
    """ Use data sent from front end to delete a row.
    Parameters
    ----------
    team_insert_entries : NBATeam[]
        Object of NBATeam class containing the data to use in update transaction.

    Returns
    -------
    stmts  : sqlalchemy.sql.dml.Insert[]
        Insert statement object that can be executed by a session.
    """
    teams_objects = []
    for team_insert_entry in team_insert_entries:
        print(f"team_insert_entry: {team_insert_entry}") #debug
        print(f"type(team_insert_entry): {type(team_insert_entry)}") #debug
        teamFromRequest = NBATeam(
                abbreviation = team_insert_entry['abbreviation'],
                city = team_insert_entry['city'],
                conference = team_insert_entry['conference'],
                division = team_insert_entry['division'],
                full_name = team_insert_entry['full_name'],
                name = team_insert_entry['name'],
                date_founded = dt.parse(team_insert_entry['date_founded']).date()
            )
        # stmt = (
        #     insert(nbaTeams).values(
        #         abbreviation = team_insert_entry.abbreviation,
        #         city = team_insert_entry.city,
        #         conference = team_insert_entry.conference,
        #         division = team_insert_entry.division,
        #         full_name = team_insert_entry.full_name,
        #         name = team_insert_entry.name,
        #         date_founded = team_insert_entry.date_founded
        #     )
        # )
        teams_objects.append(teamFromRequest)

    print(teams_objects) # debug
    return teams_objects


@app.route("/", methods=['GET','POST'])
@cross_origin()
def index():
    # with open('IC_data.json','r') as f:
    #     data = json.load(f)
    # return jsonify(data)
    res = request.json
    print(f'posted data: {res}')
    return ''


@app.route("/getData")
@cross_origin()
def retrieveData():
    with Session() as session: 
        rows = session.query(nbaTeams).all()
        list_of_dict = []
        for row in rows:
            list_of_dict.append(row._asdict())
        
        json_list_of_dict = json.dumps(list_of_dict, indent=4, sort_keys=True, default=str)
        return json_list_of_dict

@app.route("/editCellData", methods=['GET','POST'])
@cross_origin()
def editCellData():
    if request.method == 'POST':
        print('hello from editCellData ')
        data = request.data
        #pprint(request.__dict__)
        return request.data

@app.route("/deleteTeam", methods=['GET','POST','PUT','DELETE'])
@cross_origin()
def deleteTeamData():
    with Session() as session: 
        team_entry = request.json
        
        print(f"request.json {request.json}")
        print(f"team_entry[0] {team_entry[0]}")
        print(f"type(team_entry[0]) {type(team_entry[0])}")
        print(f"type(team_entry) {type(team_entry)}")

        stmt = delete(nbaTeams).where(nbaTeams.c.id.in_(team_entry))
        print(f"delete stmt: {stmt}")
        session.execute(stmt)
        session.commit()
        return request.data

@app.route("/insertTeams", methods=['POST'])
@cross_origin()
def insertTeamData():
    with Session() as session: 
        team_insert_entries = request.json
        
        # print(f"request.json {request.json}")
        # print(f"team_entry[0] {team_insert_entries[0]}")
        # print(f"type(team_entry[0]) {type(team_insert_entries[0])}")
        # print(f"type(team_entry) {type(team_insert_entries)}")


        team_objects = insertRows(team_insert_entries)
        print(f"session.add_all: {session.add_all}")
        session.add_all(team_objects)
        session.commit()
        return request.data

@app.route("/updateTeam/<int:id>", methods=['PUT'])
@cross_origin()
def updateTeamData( id=None):
    with Session() as session: 
        try:
            team_entry = request.json
            print(team_entry)
            print(f'type(team_entry): {type(team_entry)}')
            if team_entry['date_founded'] is not None:
                print(f"type(team_entry['date_founded']): {type(dt.parse(team_entry['date_founded']))}") #debug
                print(f"dt.parse(team_entry['date_founded']): {dt.parse(team_entry['date_founded'])}") #debug
                date_founded = dt.parse(team_entry['date_founded'])
            else:
                date_founded = None

            
            
            #instanciate a NBATeam object with data received
            teamFromRequest = NBATeam(
                id = team_entry['id'],
                abbreviation = team_entry['abbreviation'],
                city = team_entry['city'],
                conference = team_entry['conference'],
                division = team_entry['division'],
                full_name = team_entry['full_name'],
                name = team_entry['name'],
                date_founded = date_founded
            )

            print(f'type(teamFromRequest): {type(teamFromRequest)}') #debug
            print(f'teamFromRequest: {teamFromRequest}') #debug
            


            stmt = updateRow(teamFromRequest)
            print(f'stmt: {stmt}') #debug
            session.execute(stmt)
            session.commit()
            return '', 200
        except Exception as e:
            print(f"Excpetion : {e}") # debug
            return '', 500


@app.route("/cities")
@cross_origin()
def getCityData():
    with Session() as session: 
        rows = session.query(nbaTeams.c.city).distinct()
        list_of_cities = []
        for row in rows:
            list_of_cities.append(row._asdict())
        json_list_of_cities = json.dumps(list_of_cities)
        print(json_list_of_cities)
        return json_list_of_cities

@app.route("/conferences")
@cross_origin()
def getConferenceData():
    with Session() as session: 
        rows = session.query(nbaTeams.c.conference).distinct()
        list_of_conferences = []
        for row in rows:
            list_of_conferences.append(row._asdict())
        json_list_of_conferences = json.dumps(list_of_conferences)
        print(json_list_of_conferences)
        return json_list_of_conferences

@app.route("/divisions")
@cross_origin()
def getDivisionData():
    with Session() as session: 
        rows = session.query(nbaTeams.c.division).distinct()
        list_of_divisions = []
        for row in rows:
            list_of_divisions.append(row._asdict())
        json_list_of_divisions = json.dumps(list_of_divisions)
        print(json_list_of_divisions)
        return json_list_of_divisions




if __name__ == "__main__":
    app.run(port=5000)

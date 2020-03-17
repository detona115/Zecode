# -*- encoding: utf-8 -*-

#import sqlite3
#from sqlite3 import Error

import ast

from zecode import *

from PyQt5.QtWidgets import QMainWindow, QApplication, qApp, QTableWidgetItem # python -m pip install PyQt5
from PyQt5.QtCore import QUrl # python -m pip install PyQtWebEngine

from geojson import MultiPolygon # python -m pip install geojson
from geojson import Point
from geojson import dumps
from geojson import loads

import psycopg2 # python -m pip install psycopg2
from psycopg2 import Error
from psycopg2.extras import Json

import math # built-in

from geopy import distance # python -m pip install geopy


class MyForm(QMainWindow):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        
        #self.ui.actionSair.triggered.connect(qApp.quit)

        self.ui.spinBoxId.clear()

        ### Definindo a query que cria a tabela da base de dado

        query = "CREATE TABLE IF NOT EXISTS partners (\
        id serial NOT NULL PRIMARY KEY,\
        tradingName varchar(100),\
        ownerName varchar(100),\
        document varchar(18) UNIQUE NOT NULL,\
        corverageArea jsonb,\
        address jsonb);"

        self.createDatabase(query)

        ### Ajustando o tamanho de cada célula da tabela

        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(3, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(4, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(5, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(6, QtWidgets.QHeaderView.Stretch)
        self.ui.tableWidget.horizontalHeader().setSectionResizeMode(7, QtWidgets.QHeaderView.Stretch)

        ### Eventos de cada Butões

        ## salvar no banco de dados as coordenadas de um polygon
        self.ui.pushButtonSaveEntry.clicked.connect(self.saveEntry)

        ## listar dados de todos os parceiros da base de dados
        self.ui.pushButtonListPartners.clicked.connect(self.listPartners)

        ## apagar tabela
        self.ui.pushButtonClear.clicked.connect(self.clear)

        ## buscar partner pelo id
        self.ui.pushButtonSearchById.clicked.connect(self.getById)

        ## buscar pela latitude e longitude
        self.ui.pushButtonSearchByCoordinates.clicked.connect(self.getByCoordinates)        
                
        ## mostrando googlemaps no app
        self.dispMap()

        
        self.show()

    def connection(self):
        try:
        # conectando ao postgres server e criando a base de dado delivery
            conn = psycopg2.connect(host="localhost", user="postgres", password="1234", database="delivery")
            conn.autocommit = True
                   
        except Error as identifier:
            self.statusBar().setStyleSheet("color: orange")
            self.statusBar().showMessage("Postgres server :'"+str(identifier)+"'", 4000)
            #print("Erro ao conectar com postgres server :", identifier)
        else:
            return conn
    
    ### Tab create database

    def createDatabase(self, query):
    ### função que tem por objetivo criar a base de dado se não existir
    ### e criar a tabela

        try:
        # conectando ao postgres server e criando a base de dado delivery
            conn = psycopg2.connect(host="localhost", user="postgres", password="1234")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("CREATE DATABASE delivery;")       
        except Error as identifier:
            self.statusBar().setStyleSheet("color: orange")
            self.statusBar().showMessage("Postgres server :'"+str(identifier)+"'", 4000)
            #print("Erro ao conectar com postgres server :", identifier)
        finally:
            cur.close()
            conn.close()


        # se a conexão com postgres server for um sucesso                  
        try:
            # conectar na base de dado recém criada            
            cur = self.connection().cursor()
            # criar a tabela partners
            cur.execute(query)            

        except Error as errDb:
            # se a base de dados já existe continuar
            # senão parar no except            
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Error creating table partner :'"+str(errDb)+"'", 2000)
            #print("erro ao criar a tabela partner : ", errDb)
        finally:
            cur.close()
            self.connection().close()            
        
    #### Tab Home

    def dispMap(self, lng = 0, lat = 0):

        if lng == 0 or lat == 0:
            # se nenhuma lat ou lng for enviada mostrar a posição do usuario no mapa
            URL="https://www.google.com/maps/@?api=1&map_action=map"
        else:
            # se tiver uma determinada lat e lng, apontar a localização no google maps
            URL="https://www.google.com/maps/search/?api=1&query="+str(lat)+","+str(lng)

        self.ui.widget.load(QUrl(URL))
        #self.statusBar().setStyleSheet("color: green")
        #self.statusBar().showMessage("Maps loaded successfuly",5000)
    
    def getById(self):
        ### funçao que busca o partner pelo ID
        
        ## criando a query que vai retornar a string json que depois vai ser
        ## convertida em um dicionario python para poder imprimir e extrair os dados        

        # si a linha de busca for vazia retornar o erro na barra de status
        if self.ui.lineEditIdPartner.text() == "":
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Nothing to be searched", 2000)
        # si a linha tiver algum dado
        else:

            try:
                # verficando se a entrada da busca he um int
                type(int(self.ui.lineEditIdPartner.text()))
            except ValueError as identifier:
                self.statusBar().setStyleSheet("color: red")
                self.statusBar().showMessage("Data delivered isn't compatible with int format!", 2000)
            else:
                query = "SELECT \
                id, tradingName, ownerName, document,\
                corveragearea ->> 'type',\
                corveragearea ->> 'coordinates',\
                address ->> 'type',\
                address ->> 'coordinates'\
                FROM partners WHERE id = "+self.ui.lineEditIdPartner.text()                
            
                # fazer a connexão com a base de dados
                try:
                    cur = self.connection().cursor()         
                    with cur:                                  
                        cur.execute(query)
                        # recuperando a linha do resultado em uma variavel
                        row = cur.fetchall()                        

                        # si a linha retornada for vazia então o partner não foi encontrado
                        if len(row) == 0:     
                            self.ui.textEdit.clear()                   
                            self.statusBar().setStyleSheet("color: red")
                            self.statusBar().showMessage("Partner not found!", 2000)
                        # si o partner for encontrado imprimir seus dados na janela result search                        
                        # e apontar a sua posição no mapa
                        else:                            
                            # recuperando a latitude e longitude do partner encontrado
                            lat, lng = ast.literal_eval(row[0][7])
                            # limpando a janela result research para receber novos resultados
                            self.ui.textEdit.clear()
                            for data in row[0]:
                                self.ui.textEdit.append(str(data)+'\n')
                            # mostrando o mapa com a nova latitude e longitude
                            self.dispMap(lat, lng)
                            self.statusBar().setStyleSheet("color: green")
                            self.statusBar().showMessage("Partner found!", 2000)  
                    
                except Error as e:
                    self.statusBar().showMessage("Error connecting to database : '"+str(e)+"'", 5000)
                
                finally:
                    cur.close()
                    self.connection().close()

    def getByCoordinates(self):
    ### funçao que busca o partner mais perto pelas coordenadas

        # se uma das linhas longitude ou latitude não for preenchida retorna o erro na barra de status
        if self.ui.lineEditLongitude.text() == "" or self.ui.lineEditLongitude.text() == "":
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Data not delivered!", 2000)
        # se os dados foram preenchidos
        else:
            # se o tipo de dado fornecido não for compatível com o formato float retornar erro na barra de status
            try:
                type(float(self.ui.lineEditLongitude.text()))
                type(float(self.ui.lineEditLatitude.text()))
            except ValueError as identifier:
                self.statusBar().setStyleSheet("color: red")
                self.statusBar().showMessage("Datas delivered aren't compatible with float format!", 2000)                
                #print("Erro ao inserir valor : ", str(identifier))
            # se for compatível    
            else:
                # concatenar tudo para efetuar a busca
                lat = float(self.ui.lineEditLatitude.text())
                lng = float(self.ui.lineEditLongitude.text())                

                query = "SELECT id, address ->> 'coordinates' FROM partners"

                try:
                    cur = self.connection().cursor()
                    with cur:                                   
                        cur.execute(query)
                        # recuperando todas as coordenadas point da tabela partners
                        rows = cur.fetchall()     
                        #print(rows)
                        if rows: # se tiver partner cadastrado na base de dados, seguir com o script
                            pass
                        else: # se não tiver partner cadastrado na base de dados, imprimir na barra de status
                            raise(ValueError)
                except ValueError:
                    self.statusBar().setStyleSheet("color: red")
                    self.statusBar().showMessage("There is no partner registered inside the database! ", 4000)
                except Error as identifier:
                    self.statusBar().setStyleSheet("color: red")
                    self.statusBar().showMessage("Error connecting to database! ", 2000)                
                   # print("Erro ao conectar com o banco de dados : ", str(identifier))
                else:
                    # enviando os parametros para calcular a distancia entre cada point
                    # e retornar o id do partner mais perto
                    self.calcDistance(lat, lng, rows)
                    #pass
                finally:
                    cur.close()
                    self.connection().close()

    def calcDistance(self, lat, lng, rows):
    ## esta função tem por objetivo 
        resultado = None
        lng2 = None
        lat2 = None
        pos = None
        position = (lat, lng)
        # percorrendo todas as linhas para calcular a distancia
        # com o ponto designado
        for j, i in rows:            
            i = ast.literal_eval(i)            
            # inversão para lat e lng pois na enunciado
            # a json fornece o ponto na ordem lng lat
            i.reverse()            
            lat2 , lng2 = i # atribuição dos valores lat e lng
            position2 = (lat2, lng2)            
            #print(distance.distance(position, position2).km)
            if resultado == None:
                # se o resultado estiver sem nada 
                resultado = distance.distance(position, position2).km
                pos = j
                lat3, lng3 = position2
            else:
                if resultado > distance.distance(position, position2).km:
                    # se encontrar uma distancia menor
                    resultado = distance.distance(position, position2).km
                    pos = j
                    lat3, lng3 = position2
        #print(pos, resultado)

        # mostrando no mapa a posição do partner mais perto
        self.dispMap(lng3, lat3)

        # imprimindo os dados do partner mais perto no result search
        query = "SELECT \
        id, tradingName, ownerName, document,\
        corveragearea ->> 'type',\
        corveragearea ->> 'coordinates',\
        address ->> 'type',\
        address ->> 'coordinates'\
        FROM partners WHERE id = "+str(pos)              
    
        # fazer a connexão com a base de dados
        try:
            cur = self.connection().cursor()         
            with cur:                                  
                cur.execute(query)
                # recuperando a linha do resultado em uma variavel
                row = cur.fetchall()                            
                # si o partner for encontrado imprimir seus dados na janela result search                        
                # e apontar a sua posição no mapa
                if row:
                    self.ui.textEdit.clear()
                    for data in row[0]:
                        self.ui.textEdit.append(str(data)+'\n')
                    self.ui.textEdit.append("The nearest partner '"+row[0][2]+"', is %0.3f Km away from the coordinates you provided" %resultado)                    
                    self.statusBar().setStyleSheet("color: green")
                    self.statusBar().showMessage("The nearest partner has id : '"+str(pos)+"'", 5000)
                else:
                    raise(ValueError)           
        except Error as e:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Error connecting to database : '"+str(e)+"'", 5000)
        except ValueError:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("There is no partner registered inside the database", 5000)
        finally:
            cur.close()
            self.connection().close()

    #### Tab create partner

    def saveEntry(self):
    ### função que cria e salva os dados de um novo partner
        ## convertendo todos os dados em seus respectivos tipos        
        # ids = tradingName = ownerName = document = area = coverageArea = address = None
        try:

            ids = int(self.ui.spinBoxId.text())
            tradingName = self.ui.lineEditTradingName.text()
            ownerName = self.ui.lineEditOwnerName.text()
            document = self.ui.lineEditDocument.text()
            area = [ast.literal_eval(self.ui.textEditCoverageArea.toPlainText())]
            coverageArea = MultiPolygon(area)

            if not ids or not tradingName or not ownerName or not document or not area or not coverageArea:
                raise(SyntaxError)
                
            # validando as coordenadas do point
            type(float(self.ui.lineEditLongitude_2.text()))
            type(float(self.ui.lineEditLatitude_2.text()))
        except ValueError as identifier:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Datas delivered are incomplete or not compatible with their respective format!", 4000)
            #print("Erro ao inserir valor : ", str(identifier))
        except SyntaxError as sinErr:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Datas incomplete !", 4000)
            #print("Erro ao inserir valor : ", str(sinErr))
        else:
            lng = float(self.ui.lineEditLongitude_2.text())
            lat = float(self.ui.lineEditLatitude_2.text())
            address = Point((lng, lat), validate=True)            

            ## salvando o id, document e toda a json no banco de dados
            query = "INSERT INTO partners VALUES (%s, %s, %s, %s, %s, %s)"

            try:
                # testa se todos os dados estão sendo inseridos corretamente
                # caso ocorra um erro indica na barra de status
                # principalmente no de inserir um Id ou Document já existente na base de dados
                cur = self.connection().cursor()            
                with cur:                                    
                    cur.execute(query, (ids, tradingName, ownerName, document, Json(coverageArea), Json(address)))
                    self.statusBar().setStyleSheet("color: green")
                    self.statusBar().showMessage("Data inserted successfully into the table!",5000)                                  
            except Error as e:
                self.statusBar().setStyleSheet("color: red")
                self.statusBar().showMessage("Error inserting data ! : '"+str(e)+"'", 5000)
                #print("Erro ao inserir na base de dados : ", str(e))
            else:
                # caso todos os dados for inseridos de forma correta, limpar o formulario                
                self.clear(2)
            finally:                       
                cur.close()
                self.connection().close()   
        
    ### Tab List Partner
        
    def listPartners(self):
        # Esta função retorna todos os dados da base de dado
        
        query = "SELECT \
            id, tradingName, ownerName, document,\
            corveragearea ->> 'type',\
            corveragearea ->> 'coordinates',\
            address ->> 'type',\
            address ->> 'coordinates'\
            FROM partners;"

        try:
            # solicitando ao banco de dados todas as linhas contendo dados
            cur = self.connection().cursor()                   
            with cur:                
                cur.execute(query)
                rows = cur.fetchall()
                if rows:
                    # caso o banco de dados tenha algum dado salvo 
                    # retornar tudo e imprimir na tel
                    self.ui.tableWidget.setRowCount(0)
                    for rowNo, x in enumerate(rows):                    
                        self.ui.tableWidget.insertRow(rowNo)                    
                        for col, data in enumerate(x):
                            
                            oneColumn = QTableWidgetItem(str(data))
                            
                            self.ui.tableWidget.setItem(rowNo, col, oneColumn)
                else:
                    # caso contrario levantar um erro e imprimir na barra de status
                    raise(ValueError)
        except Error as e:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("Error trying to retrieve data from database!", 5000)
            #print("erro ao solicitar todos os dados : "+str(e))
        except ValueError:
            self.statusBar().setStyleSheet("color: red")
            self.statusBar().showMessage("There is no partnert registered in the database!", 5000)            
            #print("não tem dados cadastrados na base de dados")            
        finally:
            cur.close()
            self.connection().close()

    def clear(self, val=3):

        if val == 1:
            # se val = 1 apagar os campos de buscas da tab home
            pass
        elif val == 2:
            # se val = 2 apagar os campos de buscas da tab create
            self.ui.spinBoxId.clear()
            self.ui.lineEditTradingName.clear()
            self.ui.lineEditOwnerName.clear()
            self.ui.lineEditDocument.clear()
            self.ui.textEditCoverageArea.clear()
            self.ui.lineEditLongitude_2.clear()
            self.ui.lineEditLatitude_2.clear()
        else:
            # se val = 3 apagar os campos de buscas da tab list
            if self.ui.tableWidget.rowCount() == 0:
                self.statusBar().setStyleSheet("color: red")
                self.statusBar().showMessage("There is nothing to be cleared!", 2000)
            else:
                self.ui.tableWidget.clearContents()
                self.ui.tableWidget.setRowCount(0)

from cProfile import label
from turtle import goto
import pandas as pd

import os , fnmatch
import shutil
import glob

import datetime

import cx_Oracle
import time
import smtplib




def range(start, stop):
    i = start
    result = []

    label .begin
    if i == stop:
        goto.end

    result.append(i)
    i += 1
    goto .begin

    label .end
    print(result)
    return result

# range(1, 5)



def OracleLogistic():
    dsn_tns = cx_Oracle.makedsn('mtl-oracle-svr','1521',service_name='MTLT')
    conn = cx_Oracle.connect(user=r'Logistic', password='Logistic', dsn=dsn_tns, encoding="UTF-8",nencoding="UTF-8")
    cur = conn.cursor()
    
    return [cur,conn]

def OracleIntramart():
    dsn_tns = cx_Oracle.makedsn('m3imdbt01.murata.co.jp','1521',service_name='dbtst7')
    conn = cx_Oracle.connect(user=r'SCHM_IM_T202', password='SCHM_IM_T202', dsn=dsn_tns, encoding="UTF-8",nencoding="UTF-8")
    cur = conn.cursor()
    
    return [cur,conn]
def insertText(pathforinsert) :

    sql = "insert into IMFR_UT_MTLGLS0000( " + \
        'IMFR_UD_FLAG,' + \
        'IMFR_UD_PROC_CLS,' + \
        'IMFR_UD_DIST_CD,' + \
        'IMFR_UD_FAC_CD,' + \
        'IMFR_UD_INSP_NO,' + \
        'IMFR_UD_SEQ_NO,' + \
        'IMFR_UD_MURATATYPE,' + \
        'IMFR_UD_CUST_CD_PS,' + \
        'IMFR_UD_INSP_GUA,' + \
        'IMFR_UD_PKG_CD,' + \
        'IMFR_UD_COUNTRY_CD_OF_ORG,' + \
        'IMFR_UD_H30,' + \
        'IMFR_UD_PROD_GUA,' + \
        'IMFR_UD_STOCK_QTY,' + \
        'IMFR_UD_LOT_NO,' + \
        'IMFR_UD_FIRST_INSP_DATE,' + \
        'IMFR_UD_PREV_INSP,' + \
        'IMFR_UD_MIXED_CLS,' + \
        'IMFR_UD_PKG_COND,' + \
        'IMFR_UD_LIMIT_CLS,' + \
        'IMFR_UD_FRAC_CD,' + \
        'IMFR_UD_STD_QTY,' + \
        'IMFR_UD_BIN_NO,' + \
        'IMFR_UD_LS_DIST_CD,' + \
        'IMFR_UD_CUST_PART,' + \
        'IMFR_UD_SEQ_STK_CTL,' + \
        'IMFR_UD_REEL_ID_FLG,' + \
        'IMFR_UD_STK_CTL,' + \
        'IMFR_UD_BOX_ID,' + \
        'IMFR_UD_SERIAL_NO,' + \
        'IMFR_UD_PKG_CNT,' + \
        'IMFR_UD_CUST_PART_NEC )' + \
        'Values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32)'         
    

    data = []
    # files = "D:/BOXDATA_IMPORT/LCLW100LZR_B31"
    with open(pathforinsert) as csv_file:
        lines = csv_file.readlines()
        for a in lines :
                COMPLETE_CLASSIFICATION = a[0:1].strip()
                PROCESSING_CLASSIFICATION = a[1:2].strip()
                DISTINCTION_CODE = a[2:3].strip()
                FCTY_CD =  a[3:6].strip()
                INSPECTION_NO = a[6:16].strip()
                SEQ_NO = a[16:20].strip()
                MURATA_PART = a[20:55].strip()
                CUST_CD = a[55:60].strip()
                INSPEC_GUAR_DATE = a[60:70].strip()
                PACKING_CODE = a[70:77].strip()
                COUNTRY_CODE = a[77:80].strip()
                H30 = a[80:86].strip()
                PRODUCT_PACKAGE_DATE = a[86:101].strip()
                QUANTITY = a[101:116].strip()
                LOT_NO = a[116:126].strip()
                FIRST_INSPECTION_DATE = a[126:136].strip()
                PREVIOUS_INSPECTION_NO = a[136:146].strip()
                MIXED_LOADING = a[146:147].strip()
                PRODUCT_PACKAGE_CON = a[147:148].strip()
                LIMITED_SHIPMENT  = a[148:150].strip()
                PRODUCT_PACKAGE_FRACTION  = a[150:151].strip()
                STANDARD_QTY  = a[151:166].strip()
                BIN_NO = a[166:176].strip()
                LC_STOCK_CODE = a[176:177].strip()
                CUSTOMER_PART = a[177:203].strip()
                SEQ_NUMBER_STOCK = a[203:217].strip()
                BARCODE_ID = a[217:218].strip()
                STOCK_CONTROL = a[218:233].strip()
                BOX_ID = a[233:237].strip()
                SERIAL_NO = a[237:248].strip()
                INDICATE_PACKAGE_COUNT = a[248:255].strip()
                CUST_PART_NO_NEC = a[255:281].strip()

                data.append((COMPLETE_CLASSIFICATION,PROCESSING_CLASSIFICATION,DISTINCTION_CODE,FCTY_CD,INSPECTION_NO,SEQ_NO,MURATA_PART,CUST_CD,INSPEC_GUAR_DATE,PACKING_CODE,COUNTRY_CODE,H30,PRODUCT_PACKAGE_DATE,QUANTITY,LOT_NO,FIRST_INSPECTION_DATE,PREVIOUS_INSPECTION_NO,MIXED_LOADING,PRODUCT_PACKAGE_CON,LIMITED_SHIPMENT,PRODUCT_PACKAGE_FRACTION,STANDARD_QTY,BIN_NO,LC_STOCK_CODE,CUSTOMER_PART,SEQ_NUMBER_STOCK,BARCODE_ID,STOCK_CONTROL,BOX_ID,SERIAL_NO,INDICATE_PACKAGE_COUNT,CUST_PART_NO_NEC))
        #ถ้ามีดาต้าให้ Insert
        if data:
            cur = OracleIntramart()
            cur[0].executemany(sql, data)
            cur[1].commit()
            print(" >>> COMMIT INSERT DATA")

    os.remove(pathforinsert)

def clearfile(path):
     with open(path, 'w') as file:
         file.write("")

def summary_filetxt(src_folder,pattern) :
    i = 0
    file_name =  "data"
    with open(src_folder + file_name, 'w') as newtext_file:
        
        files = glob.glob(src_folder + pattern)
        for path in files:
            i += 1
            # "//163.50.57.112/gls/WindowsApplicationGLS/PathCopy/B31/"
            with open(path) as txt_file:
                txt = txt_file.read() 
                newtext_file.write(txt)
                
            os.remove(path)
        newtext_file.close()
  
        # if i == 0 :         
        #    deleltAllFile(file_name,file_send)
def checkSem(path , i):
    # path = "\\\\163.50.57.112\\gls\\WindowsApplicationGLS\\ftpPRASStoIM\\ftpPRASStoIM\\bin\\Debug\\data\\MKV\\LS\\send\\.sem"
    if os.path.exists(path) == True :
        
        time.sleep(5)
        print("WAIT .sem " + str(i) )
        if i == 2 : 
            # sendmail()
            return "Remain"                   
        else :
            return checkSem(path,i+1)
    else:
        return "OK"

def check_sem() :

    now = datetime.datetime.now()
    strnow = now.strftime("%Y%m%d_%H%M%S")
    strday = now.strftime("%Y%m%d")
                
    try :

        cur = OracleLogistic()
        sql = "select * from PROD_BOXCREATION order by FCTY_CD"
        cur[0].execute(sql)
        i = 0
        item = [column_name[0] for column_name in cur[0].description]
        for row in cur[0]:
            FCTY_CD = row[item.index('SEND_FILE_GLS_BOX')] #File name
            SEND_PATH_GLS_BOX = row[item.index('SEND_PATH_GLS_BOX')] #\\163.50.57.112\gls\WindowsApplicationGLS\ftpPRASStoIM\ftpPRASStoIM\bin\Debug\data\LS\send\
            SEND_FILE_GLS_BOX = row[item.index('SEND_FILE_GLS_BOX')] #LCLW100LZR_B31
            SEND_PATH_GLS_COPY_TMP_BOX = row[item.index('COPY_PATH')] #\\163.50.57.112\gls\WindowsApplicationGLS\PathCopy\B31\
            SEND_PATH_GLS_TMP_BOX = row[item.index('BACKUP_PATH')] #\\163.50.57.112\gls\WindowsApplicationGLS\PathBackup\B31\
            SEND_FILE_IM_TRIGER = row[item.index('SEND_FILE_IM_TRIGER')] #.sem
            Path_error = row[item.index('ERROR_PATH')]
            

            # check .sem
        
            checkSemfolder = SEND_PATH_GLS_BOX + SEND_FILE_IM_TRIGER 
            i = 0
            # label .begin
            print("START : " + SEND_FILE_GLS_BOX)
            # sendmail()
            if checkSem(checkSemfolder, i) != "OK" :
                message = "CAN'T INSERT INTERFACE FILE TO DATABASE >> " + FCTY_CD + " >> PATH FILE : " + SEND_PATH_GLS_BOX
                sendmail(message)
                print("End")
            else :
                originalpath = SEND_PATH_GLS_BOX + SEND_FILE_GLS_BOX
            
                if os.path.exists(originalpath) == True :
                    with open(originalpath) as csv_file:
            
                        lines = csv_file.readlines() 
                        if len(lines) > 0 :
                            #สร้าง .sem
                            os.mkdir(checkSemfolder) 
                            #copy to summary file
                            shutil.copy(originalpath, SEND_PATH_GLS_COPY_TMP_BOX + SEND_FILE_GLS_BOX + "_" + str(strnow) + ".txt")
                            #copy to backup
                            shutil.copy(originalpath, SEND_PATH_GLS_TMP_BOX + SEND_FILE_GLS_BOX + "_" + str(strnow) + ".txt")
                            #clear txt file 
                            clearfile(originalpath)
                            # file.close()
                            os.rmdir(checkSemfolder)
                            
                            summary_filetxt(SEND_PATH_GLS_COPY_TMP_BOX,"*.txt")
                            
                            insertText(SEND_PATH_GLS_COPY_TMP_BOX +"data" )

            # label .end
    except Exception as e:
        with open(Path_error + "ERROR_" + str(strday) + ".txt", 'a') as newtext_file:
            newtext_file.write("\n" + str(strnow) + ": " + str(e))

def sendmail(massage):
    sql = "select IMFR_UD_SEND_TO , IMFR_UD_SEND_FROM, IMFR_UD_SUBJECT from imfr_ut_mtlglsmail WHERE IMFR_UD_APP_ID = 'mtlgls0000' "
    cur = OracleIntramart()
    cur[0].execute(sql)
    i = 0
    item = [column_name[0] for column_name in cur[0].description]
    for row in cur[0]:
        IMFR_UD_SEND_TO = row[item.index('IMFR_UD_SEND_TO')]
        IMFR_UD_SEND_FROM = row[item.index('IMFR_UD_SEND_FROM')]
        IMFR_UD_SUBJECT = row[item.index('IMFR_UD_SUBJECT')]
    sender = IMFR_UD_SEND_FROM
    receivers = [IMFR_UD_SEND_TO]
    

    SUBJECT = IMFR_UD_SUBJECT
 
  


    message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\

    %s
    """ % (sender, ", ".join(receivers), SUBJECT, massage)


    server = smtplib.SMTP("172.24.128.80")
    server.sendmail(sender, receivers, message)
    server.quit()
       

check_sem()


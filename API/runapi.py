from flask import Flask
from flask import render_template
from flask.ext.wtf import Form
#from flask_wtf import Form
from wtforms.fields import TextField, BooleanField
from wtforms.validators import Required
from flask import request, redirect, session, g, url_for, abort, render_template, flash

import json
import pygal
import happybase
import shutil

app = Flask(__name__)

#function takes the date in the format MM/DD/YYYY and transforms it to the format YYYY-MM-DD
def date_transform(dt):
    if dt:
        month, day, year = dt.split('/')
        return year + '-' + month + '-' + day
    else:
        return ''

#returns a dictioanary of keys assume that transformed dates are passed
def row_keys(date_from, date_to):
    #if value was passed to the function
    if date_from and date_to:
        row_key_dict = {'dates1':[], 'months1':[], 'years':[], 'months2':[], 'dates2':[]}
        year_from, month_from, day_from = date_from.split('-')
        year_to, month_to, day_to = date_to.split('-')
    
        #if there is at most one year difference don't bother processing years separately
        if int(year_to) - int(year_from) < 2:
            #the dates are within the same year and there is less than 2 months difference
            if (int(year_to) == int(year_from)) and (int(month_to) - int(month_from) < 2):
                row_key_dict['dates1'].append(date_from)
                row_key_dict['dates1'].append(date_to)
            #there is at least one full month to be processed
            else:
                mnth_tmp = (int(month_from)+1)%12
                #calculation goes to the first of the next month because end_row key in happy base scan method is exclusive
                date_tmp = year_from + '-' + '%02d'%mnth_tmp + '-' + '01'
                row_key_dict['dates1'].append(date_from)
                row_key_dict['dates1'].append(date_tmp)
                
                #generate corresponding keys for the month
                tmp_month = year_from + '-' + '%02d'%mnth_tmp
                row_key_dict['months1'].append(tmp_month)
                row_key_dict['months1'].append(year_to + '-' + month_to)
                
                #process the dates in the last month
                row_key_dict['dates2'].append(year_to + '-' + month_to + '-' + '01')
                row_key_dict['dates2'].append(date_to)
        #at least one year is going to be processed differently
        else:
            tmp_mnth = (int(month_from)+1)%12
            tmp_date = year_from + '-' + '%02d'%tmp_mnth + '-' + '01'
            row_key_dict['dates1'].append(date_from)
            row_key_dict['dates1'].append(tmp_date)
            
            row_key_dict['months1'].append(year_from + '-' + '%02d'%tmp_mnth)
            row_key_dict['months1'].append(str((int(year_from)+1)) + '-' + '01')
            
            row_key_dict['years'].append(str(int(year_from)+1))
            row_key_dict['years'].append(year_to)
            
            row_key_dict['months2'].append(year_to + '-' + '01')
            row_key_dict['months2'].append(year_to + '-' + month_to)
            
            row_key_dict['dates2'].append(year_to + '-' + month_to + '-' + '01')
            #here we are loosing one day since the last day is not inclusive correct it later
            row_key_dict['dates2'].append(date_to)
        
        return row_key_dict
    else:
        return None

    
@app.route('/', methods=['GET', 'POST'])
def index():
    #the next 3 lines might be deleted at some point
    airline = "AA"
    date_from = "2014/06/01"
    date_to = "2014/07/31"
    tmp={'airline': airline, 'from': date_transform(date_from), 'to': date_transform(date_to), 'report_type':'delay_category'}
    chart_data = delay_category(tmp)
    if request.method == 'POST':
        if request.form['report_type'] == 'delay_category':
            airline = request.form['airline']
            date_from = request.form['from']
            date_to = request.form['to']
            tmp={'airline': airline, 'from': date_transform(date_from), 'to': date_transform(date_to), 'report_type':'delay_category'}
            chart_data = delay_category(tmp)
        elif request.form['report_type'] == 'delay_holiday':
            airline = request.form['airline']
            holiday = request.form['holiday']
            tmp={'airline': airline, 'holiday': holiday, 'report_type':'delay_holiday'}
            chart_data = delay_holiday(tmp)
        elif request.form['report_type'] == 'delay_career':
            date_from = request.form['from']
            date_to = request.form['to']
            tmp={'from': date_transform(date_from), 'to': date_transform(date_to), 'report_type':'delay_career'}
            chart_data = airline_delays(tmp)
        elif request.form['report_type'] == 'delay_flightNum':
            airline = request.form['airline']
            flightNum = request.form['flightNum']
            date_from = request.form['from']
            date_to = request.form['to']
            tmp={'airline': airline, 'from': date_transform(date_from), 'to': date_transform(date_to), 'flightNum': flightNum, 'report_type':'delay_flightNum'}
            chart_data = flightNum_delay(tmp)
        elif request.form['report_type'] == 'delay_season':
            year = request.form['year']
            flightNum = request.form['flightNum']
            airline = request.form['airline']
            tmp={'airline': airline, 'year': year, 'flightNum': flightNum, 'report_type':'delay_season'}
            chart_data = season_delay(tmp)

    return render_template('index.html', data=tmp, chart_data=chart_data)


def delay_category(form_data):
    categ_total = []
    #count the number of delays per each category
    delay = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    #calculate the number of flights for a particular airline in the provided timeframe
    flight_total = 0
    delay_total = 0

    #get row keys  for particular dates
    rk_dict = row_keys(form_data['from'], form_data['to'])
    arln = form_data['airline'].upper()
    
    connection = happybase.Connection('localhost')
    connection.open()

    if rk_dict is not None:
        if len(rk_dict['years']) != 0:
            table = connection.table('delay_year_count')
            rk_start = arln + '_' + rk_dict['years'][0]
            rk_end = arln + '_' + rk_dict['years'][1]
       
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('year_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['months1']) != 0:
            table = connection.table('delay_month_count')
            rk_start = arln + '_' + rk_dict['months1'][0]
            rk_end = arln + '_' + rk_dict['months1'][1]
       
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('month_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['months2']) != 0:
            table = connection.table('delay_month_count')
            rk_start = arln + '_' + rk_dict['months2'][0]
            rk_end = arln + '_' + rk_dict['months2'][1]
       
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('month_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['dates1']) != 0:
            table = connection.table('delay_date_count')
            rk_start = arln + '_' + rk_dict['dates1'][0]
            rk_end = arln + '_' + rk_dict['dates1'][1]
       
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('date_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])


        if len(rk_dict['dates2']) != 0:
            table = connection.table('delay_date_count')
            rk_start = arln + '_' + rk_dict['dates2'][0]
            rk_end = arln + '_' + rk_dict['dates2'][1]
       
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('date_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                    flight_total += int(data['count:Num_Flights'])

        connection.close()

        #convert the dictionary to a list and calculate the percentages
        for key, value in delay.iteritems():
            temp = [key,value]
            delay_total += value
            if value >= 0.01:
                categ_total.append(temp)
    
        #calculate the number of flights that were not delayed
        notDelayed_total = flight_total - delay_total
        el = ['On Time', notDelayed_total]
        categ_total.insert(0,el)

        #calculate the percentages
        for x in categ_total:
            if flight_total != 0:
                x[1] = x[1]/float(flight_total)

        return categ_total
    else:
        return None

def flightNum_delay(form_data):
    categ_total = []
    #count the number of delays per each category
    delay = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    #calculate the number of flights for a particular airline and flight number in the provided timeframe
    flight_total = 0
    delay_total = 0
    
    #get row keys  for particular dates
    rk_dict = row_keys(form_data['from'], form_data['to'])
    arln = form_data['airline'].upper()
    flNum = form_data['flightNum']
    
    connection = happybase.Connection('localhost')
    connection.open()
    
    if rk_dict is not None:
        if len(rk_dict['years']) != 0:
            table = connection.table('delay_flightNum_year')
            rk_start = arln + '_' + flNum + '_' + rk_dict['years'][0]
            rk_end = arln + '_' + flNum + '_' + rk_dict['years'][1]
           
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])
            
            table = connection.table('flightNum_year_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['months1']) != 0:
            table = connection.table('delay_flightNum_month')
            rk_start = arln + '_'+ flNum + '_'  + rk_dict['months1'][0]
            rk_end = arln + '_'+ flNum + '_'  + rk_dict['months1'][1]
           
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('flightNum_month_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['months2']) != 0:
            table = connection.table('delay_flightNum_month')
            rk_start = arln + '_'+ flNum + '_'  + rk_dict['months2'][0]
            rk_end = arln + '_'+ flNum + '_'  + rk_dict['months2'][1]
           
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('flightNum_month_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        if len(rk_dict['dates1']) != 0:
            table = connection.table('delay_flightNum_date')
            rk_start = arln + '_'+ flNum + '_'  + rk_dict['dates1'][0]
            rk_end = arln + '_' + flNum + '_' + rk_dict['dates1'][1]
           
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('flightNum_date_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])


        if len(rk_dict['dates2']) != 0:
            table = connection.table('delay_flightNum_date')
            rk_start = arln + '_'+ flNum + '_'  + rk_dict['dates2'][0]
            rk_end = arln + '_' + flNum + '_' + rk_dict['dates2'][1]
           
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                delay['NAS'] += int(data['delay:NASDelay_Num'])
                delay['Security'] += int(data['delay:SecurityDelay_Num'])
                delay['Weather'] += int(data['delay:WeatherDelay_Num'])
                delay['Late Aircraft'] += int(data['delay:LateAircraftDelay_Num'])
                delay['Carrier'] += int(data['delay:CarrierDelay_Num'])
                delay['Unclassified'] += int(data['delay:OtherDelay_Num'])

            table = connection.table('flightNum_date_fcount')
            for key, data in table.scan(row_start = rk_start, row_stop = rk_end):
                flight_total += int(data['count:Num_Flights'])

        connection.close()
        
        #convert the dictionary to a list and calculate the percentages
        for key, value in delay.iteritems():
            temp = [key,value]
            delay_total += value
            if value >= 0.01:
                categ_total.append(temp)
        
        #calculate the number of flights that were not delayed
        notDelayed_total = flight_total - delay_total
        el = ['On Time', notDelayed_total]
        categ_total.insert(0,el)
        
        #calculate the percentages
        for x in categ_total:
            if flight_total != 0:
                x[1] = x[1]/float(flight_total)

        return categ_total
    else:
        return None

def season_delay(form_data):
    # delay_types = ['NAS', 'Security', 'Weather', 'Late Aircraft', 'Carrier', 'Unclassified']
    season_total = []
    flight_total = [0, 0, 0, 0]
    #  season_total.append(delay_types)
    #count the total number of delays per each season
    delay_fall = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    delay_winter = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    delay_spring = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    delay_summer = {'NAS':0, 'Security':0, 'Weather':0, 'Late Aircraft':0, 'Carrier':0, 'Unclassified':0}
    #calculate the number of flights for a particular airline and flight number per season
    onTime_total = []
    fall = [0, 0, 0, 0, 0, 0]
    winter = [0, 0, 0, 0, 0, 0]
    spring = [0, 0, 0, 0, 0, 0]
    summer = [0, 0, 0, 0, 0, 0]
    data_exist = False
    
    #assosiate seasons with quarters
    #   season_quarter = {'1':'Fall', '2': 'Winter', '3':'Spring', '4':'Summer'}
    
    #define the row key
    row_key = form_data['airline'].upper() + '_' + form_data['flightNum'] + '_' + form_data['year'] + '_'
    rk_fall = row_key + '1'
    rk_winter = row_key + '2'
    rk_spring = row_key + '3'
    rk_summer = row_key + '4'
    
    if row_key is not None:
        connection = happybase.Connection('localhost')
        connection.open()
        table = connection.table('quarterly_delays')
        
        #get the data for fall
        data = table.row(rk_fall)
        if len(data) != 0:
            data_exist = True
            fall[0] += int(data['delay:NASDelay_Num'])
            fall[1] += int(data['delay:SecurityDelay_Num'])
            fall[2] += int(data['delay:WeatherDelay_Num'])
            fall[3] += int(data['delay:LateAircraftDelay_Num'])
            fall[4] += int(data['delay:CarrierDelay_Num'])
            fall[5] += int(data['delay:OtherDelay_Num'])
        
        #get the data for winter
        data = table.row(rk_winter)
        if len(data) != 0:
            winter[0] += int(data['delay:NASDelay_Num'])
            winter[1] += int(data['delay:SecurityDelay_Num'])
            winter[2] += int(data['delay:WeatherDelay_Num'])
            winter[3] += int(data['delay:LateAircraftDelay_Num'])
            winter[4] += int(data['delay:CarrierDelay_Num'])
            winter[5] += int(data['delay:OtherDelay_Num'])

    
        #get the data for spring
        data = table.row(rk_spring)
        if len(data) != 0:
            spring[0] += int(data['delay:NASDelay_Num'])
            spring[1] += int(data['delay:SecurityDelay_Num'])
            spring[2] += int(data['delay:WeatherDelay_Num'])
            spring[3] += int(data['delay:LateAircraftDelay_Num'])
            spring[4] += int(data['delay:CarrierDelay_Num'])
            spring[5] += int(data['delay:OtherDelay_Num'])
    
        #get the data for summer
        data = table.row(rk_summer)
        if len(data) != 0:
            summer[0] += int(data['delay:NASDelay_Num'])
            summer[1] += int(data['delay:SecurityDelay_Num'])
            summer[2] += int(data['delay:WeatherDelay_Num'])
            summer[3] += int(data['delay:LateAircraftDelay_Num'])
            summer[4] += int(data['delay:CarrierDelay_Num'])
            summer[5] += int(data['delay:OtherDelay_Num'])
        
        if data_exist:
            #get the total number of flights for a specific query
            table = connection.table('quarterly_delays_fcount')
            #process Fall
            data = table.row(rk_fall)
            if len(data) != 0:
                flight_total[0] += int(data['count:Num_Flights'])
            #process Winter
            data = table.row(rk_winter)
            if len(data) != 0:
                flight_total[1] += int(data['count:Num_Flights'])
            #process Spring
            data = table.row(rk_spring)
            if len(data) != 0:
                flight_total[2] += int(data['count:Num_Flights'])
            #process Summer
            data = table.row(rk_summer)
            if len(data) != 0:
                flight_total[3] += int(data['count:Num_Flights'])
            
            #calculate the total number of flights that were on time for each season and insert it as a first element
            total_del = 0
            for i in fall:
                total_del += i
            #calculate the number of on time flights for fall
            fall.insert(0, flight_total[0]-total_del)
            season_total.append(fall)
            
            total_del = 0
            for i in winter:
                total_del += i
            #calculate the number of on time flights for winter
            winter.insert(0, flight_total[1]-total_del)
            season_total.append(winter)
            
            total_del = 0
            for i in spring:
                total_del += i
            #calculate the number of on time flights for spring
            spring.insert(0, flight_total[2]-total_del)
            season_total.append(spring)

            total_del = 0
            for i in summer:
                total_del += i
            #calculate the number of on time flights for fall
            summer.insert(0, flight_total[3]-total_del)
            season_total.append(summer)

        return season_total
    else:
        return None




def airline_delays(form_data):
    #define the available airlines
    airline_list = ["9E", "AA", "AQ", "AS", "B6", "CO", "DH", "DL", "E*", "EV", "F9", "FL", "HA", "HP", "MQ", "NW", "OH", "OO", "TW", "TZ", "UA", "US", "VX", "WN", "XE", "YV"]
    flight_total = []
    delay_list = []
    exist_air = []
    #count the number of delays for the required categories
    flight_info = {'Carrier':0, 'Total':0}
    #calculate the number of flights for a particular airline in the provided timeframe
   
    row_keys_start = []
    row_keys_end = []
    
    #check if the dates were filled in the form
    if form_data['from'] and form_data['to']:
    
        #construct row keys for all airlines
        for x in airline_list:
            row_keys_start.append(x + '_' + form_data['from'])
            row_keys_end.append(x + '_' + form_data['to'])
        
        connection = happybase.Connection('localhost')
        connection.open()

        air_index = 0
        #iterate through row keys and make the necessary structures
        for x,y in zip(row_keys_start, row_keys_end):
            #caluclate totals for the next pair
            flight_info['Carrier'] = 0
            flight_info['Total'] = 0
            if x and y:
                table = connection.table('delay_date_count')
                for key, data in table.scan(row_start = x, row_stop = y):
                    flight_info['Carrier'] += int(data['delay:CarrierDelay_Num']) + int(data['delay:LateAircraftDelay_Num']) + int(data['delay:OtherDelay_Num'])
            
                table1 = connection.table('date_fcount')
                for key, data in table1.scan(row_start = x, row_stop = y):
                    flight_info['Total'] += int(data['count:Num_Flights'])
                
                #check if there were any flights by that airline
                if flight_info['Total'] > 0:
                    delay_list.append(flight_info['Carrier']/float(flight_info['Total']))
                    exist_air.append(airline_list[air_index])

                air_index += 1 #parse next airline

        flight_total.append(exist_air)
        flight_total.append(delay_list)
       
        connection.close() #done processing records
       
        return flight_total
    else:
        return None

def delay_holiday(form_data):
    #define holidays that need to be processed
    holidays = {'New Year': '01-01', 'Christmas':'12-25', 'Independence Day':'07-04'}
    #define years to be processed
    years = range(1987, 2014)
    holiday_total = []
    holiday_total.append(years)
    #count the number of delays per each category
    delayH = []
    delay15 = []
    #calculate the number of flights for a particular airline in the provided timeframe
    flight_total_hol = []
    flight_total_15 = []
    delay_ratio_hol = []
    delay_ratio_15 = []
    
    arln = form_data['airline'].upper()
    hol = form_data['holiday'] #get the holiday
 
    month, day = holidays[hol].split('-') # get the month and the day of the holiday
    rk_holiday = arln + '_'
    rk_day15 = arln + '_'
    if rk_holiday and rk_day15:
        connection = happybase.Connection('localhost')
        connection.open()
        
        table = connection.table('delay_date_count')
        table2 = connection.table('date_fcount')
        for x in range(1987, 2014):
            rk_holiday += str(x)+'-'+holidays[hol] #construct the row key to query the htable
            rk_day15 += str(x) + '-' + month + '15' #get the 15th day of the same month and year
            #calculate the total number of delays for the holiday from hbase
            res1 = table.row(rk_holiday)
            el = 0
            if res1:
                el += int(res1['delay:NASDelay_Num']) + int(res1['delay:SecurityDelay_Num']) + int(res1['delay:WeatherDelay_Num']) + int(res1['delay:LateAircraftDelay_Num']) + int(res1['delay:CarrierDelay_Num']) + int(res1['delay:OtherDelay_Num'])
            delayH.append(el)
            
            #calculate the total number of delays for the 15th of the month
            res2 = table.row(rk_day15)
            el = 0
            if res2:
                el += int(res2['delay:NASDelay_Num']) + int(res2['delay:SecurityDelay_Num']) + int(res2['delay:WeatherDelay_Num']) + int(res2['delay:LateAircraftDelay_Num'])+ int(res2['delay:CarrierDelay_Num']) + int(res2['delay:OtherDelay_Num'])
            delay15.append(el)
        
            #get the total number of flight for each of the days
            res1 = table2.row(rk_holiday)
            if res1:
                flight_total_hol.append(int(res1['count:Num_Flights']))
            else:
                flight_total_hol.append(0)
    
            res2 = table2.row(rk_holiday)
            if res2:
                flight_total_15.append(int(res2['count:Num_Flights']))
            else:
                flight_total_15.append(0)

        
        connection.close()
        
        for i in range(0,len(delayH)):
            if flight_total_hol[i] != 0:
                delay_ratio_hol.append(delayH[i]/float(flight_total_hol[i]))
            else:
                delay_ratio_hol.append(0)
            if flight_total_15[i] != 0:
                delay_ratio_15.append(delay15[i]/float(flight_total_15[i]))
            else:
                delay_ratio_15.append(0)

        holiday_total.append(delay_ratio_hol)
        holiday_total.append(delay_ratio_15)

        return holiday_total
    else:
        return None

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5005,  debug=True)

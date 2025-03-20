import os
import psycopg2
import pandas as pd
import numpy as np
import yagmail
import datetime
import time
from datetime import date
from pathlib import Path


current_date2 = date.today().strftime("%d-%m-%Y")

# ---------------------------- Configuration ----------------------------

# Database Configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': int(os.getenv("DB_PORT", 5432))  # Default port if not provided
}

# Email Configuration
EMAIL_CONFIG = {
    'user': os.getenv("EMAIL_USER"),          # Sender email
    'password': os.getenv("EMAIL_PASSWORD"),  # App password or email password
    'smtp_server': 'smtp.gmail.com',          # SMTP server for Gmail
    'smtp_port': 587                          # SMTP port for TLS
}

# Email Recipients
RECIPIENTS = ['shashidhar@xhipment.com', 'srikrishna@xhipment.com']       # Add more recipients as needed

# Email Content
EMAIL_SUBJECT = f'Daily CTT Metrics Report {current_date2}'
EMAIL_BODY = 'Please find the daily CTT Metrics report attached.'

# Attachment Details
ATTACHMENT_NAME = f'CTT Metrics Report {current_date2}.xlsx'
OUTPUT_PATH = Path.cwd() / ATTACHMENT_NAME

# Establish connection
conn = psycopg2.connect(**DB_CONFIG)



accessorials_data = pd.read_sql_query(''' select *,
extract(week from created_date) as created_week, extract(month from created_date) as created_month, 
extract(quarter from created_date) as created_quarter, extract(year from created_date) as created_year,
extract(week from closed_date) as closed_week, extract(month from closed_date) as closed_month, 
extract(quarter from closed_date) as closed_quarter, extract(year from closed_date) as closed_year,
(case when ((extract(week from created_date) = extract(week from closed_date)) and (extract(year from created_date) = extract(year from closed_date))) then 'same week' else 'different week' end) as week_compare,
(case when ((extract(month from created_date) = extract(month from closed_date)) and (extract(year from created_date) = extract(year from closed_date))) then 'same month' else 'different month' end) as month_compare,
(case when ((extract(quarter from created_date) = extract(quarter from closed_date)) and (extract(year from created_date) = extract(year from closed_date))) then 'same quarter' else 'different quarter' end) as quarter_compare,
(case when extract(year from created_date) = extract(year from closed_date) then 'same year' else 'different year' end) as year_compare
from prod_xhipment_prod.ctt_wbr ''', conn)
accessorials_data_copy = pd.read_sql_query('''select * from prod_xhipment_prod.ctt_wbr''', conn)
date_dim= pd.read_sql_query("SELECT * FROM prod_xhipment_prod.date_dim_sunday", conn)

# Close connection
conn.close()


def send_email(attachment_path):
    """
    Sends the email with the Excel report as an attachment.
    """
    try:
        # Initialize the SMTP client
        yag = yagmail.SMTP(EMAIL_CONFIG['user'], EMAIL_CONFIG['password'])
        
        # Send the email
        yag.send(
            to=RECIPIENTS,
            subject=EMAIL_SUBJECT,
            contents=EMAIL_BODY,
            attachments=attachment_path
        )
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")



pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Execute the query and load the result into a wbr_source DataFrame
accessorials_data['no_of_tickets_received'] = accessorials_data[accessorials_data['tag'] == 'CTT']['ticket_id']
accessorials_data['open_tickets'] = accessorials_data[accessorials_data['status'] == 'Open']['ticket_id']

# accessorials_data['sales_poc']=accessorials_data.sales_poc.fillna('unassigned')
dtype_conv_dict={'category':'category'}
accessorials_data=accessorials_data.astype(dtype_conv_dict)

float_cols = date_dim.select_dtypes(include='float64').columns  # Select float64 columns
date_dim[float_cols] = date_dim[float_cols].astype(int)

category=accessorials_data.category.unique()
# is_send_quote=accessorials_data.is_send_quote.unique()
# origin_country=accessorials_data.origin_country.unique()
# trade_lane=accessorials_data.trade_lane.unique()
print(category)

# def generate_df(kind=kind,is_send_quote=is_send_quote,origin_country=origin_country,trade_lane=trade_lane):
def generate_df(category=category):
    data=accessorials_data[(accessorials_data.category.isin(category))].copy()
    # data=accessorials_data.copy()
            # Get today's date
    today = pd.to_datetime('today').date()
    
    # Get the weekday (Sunday=6, Monday=0, ..., Saturday=5)
    weekday = today.weekday()
    
    # Adjust to get the last nth week's start date (5 weeks ago, starting Sunday)
    last_nth_week_start_date = today - pd.Timedelta(days=(weekday + 1)) - pd.Timedelta(weeks=5)
    
    # Get the last week's end date (last Saturday)
    last_1_week_end_date = today - pd.Timedelta(days=(weekday + 2))
    
    # accessorial_week_df=date_dim[(date_dim.date.between(last_nth_week_start_date,last_1_week_end_date))].copy().merge(data,left_on='date',right_on='created_at',how='left')

    # display(data)
    last_nth_week_start_date=pd.to_datetime('today').date()-pd.Timedelta(days= pd.to_datetime('today').isocalendar()[2]-1)-pd.Timedelta(weeks=4)
    
    ctt_created_week_df=date_dim[(date_dim.date.between(last_nth_week_start_date,last_1_week_end_date))].copy().merge(data,left_on='date',right_on='created_date',how='left')
    ctt_closed_week_df=date_dim[(date_dim.date.between(last_nth_week_start_date,last_1_week_end_date))].copy().merge(data,left_on='date',right_on='closed_date',how='left')

    ctt_created_month_df=date_dim[(date_dim.year>=pd.Timestamp('today').year-1)].copy().merge(data,left_on='date',right_on='created_date',how='left')
    ctt_closed_month_df=date_dim[(date_dim.year>=pd.Timestamp('today').year-1)].copy().merge(data,left_on='date',right_on='closed_date',how='left')


    # ctt_created_week_final=ctt_created_week_df.copy().groupby(by=['week_start_date','week','week_number'],as_index=False).agg(
    #     tickets_created=('no_of_tickets_received','nunique'), tickets_open=('open_tickets','nunique'), resolution_time_current=('resolution_time','mean')
    #     ).sort_values(by=['week_start_date'],ascending=[True])[['week', 'tickets_created', 'tickets_open', 'resolution_time_current']].transpose().reset_index()

    ctt_created_week_final = ctt_created_week_df.copy().groupby(
    by=['week_start_date', 'week', 'week_number'], as_index=False
    ).agg(
        tickets_created=('no_of_tickets_received', 'nunique'),
        tickets_open=('open_tickets', 'nunique'),
        resolution_time_current=('resolution_time', 'mean'),
        net_open_tickets=('week_compare', lambda x: (x == 'different week').sum())  # Count tickets where week_compare is 'same week'
    ).sort_values(by=['week_start_date'], ascending=True)[['week', 'tickets_created', 'net_open_tickets', 'tickets_open', 'resolution_time_current']].transpose().reset_index()

    # # Compute net open tickets
    # ctt_created_week_final['net_open_tickets'] = ctt_created_week_final['tickets_created'] - ctt_created_week_final['tickets_closed_same_week']

    # # Select relevant columns for final output
    # ctt_created_week_final = ctt_created_week_final[
    # ['week', 'tickets_created', 'tickets_closed_same_week', 'net_open_tickets', 'tickets_open', 'resolution_time_current']
    # ].transpose().reset_index()

    ctt_closed_week_final=ctt_closed_week_df.copy().groupby(by=['week_start_date','week','week_number'],as_index=False).agg(
        tickets_closed=('no_of_tickets_received','nunique'), first_response_time=('first_response_time','mean'), resolution_time=('resolution_time','mean')
        ).sort_values(by=['week_start_date'],ascending=[True])[['week', 'tickets_closed', 'first_response_time', 'resolution_time']].transpose().reset_index()


    print(ctt_created_week_final)
    print(ctt_closed_week_final)

    ctt_created_week_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_created_week_final.shape[0])]
    ctt_closed_week_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_closed_week_final.shape[0])]





    ctt_created_month_final = ctt_created_month_df.copy().groupby(
    by=['month_year', 'year', 'month_number'], as_index=False
    ).agg(
        tickets_created=('no_of_tickets_received', 'nunique'),
        tickets_open=('open_tickets', 'nunique'),
        resolution_time_current=('resolution_time', 'mean'),
        net_open_tickets=('month_compare', lambda x: (x == 'different month').sum())  # Count tickets where month_compare is 'same month'
    ).sort_values(by=['year', 'month_number'], ascending=[True, True])[['month_year', 'tickets_created', 'net_open_tickets', 'tickets_open', 'resolution_time_current']].transpose().reset_index()

    # # Compute net open tickets
    # ctt_created_month_final['net_open_tickets'] = ctt_created_month_final['tickets_created'] - ctt_created_month_final['tickets_closed_same_month']

    # # Select relevant columns for final output
    # ctt_created_month_final = ctt_created_month_final[
    # ['month_year', 'tickets_created', 'tickets_closed_same_month', 'net_open_tickets', 'tickets_open', 'resolution_time_current']
    # ].transpose().reset_index()

    ctt_closed_month_final=ctt_closed_month_df.copy().groupby(by=['month_year','year','month_number'],as_index=False).agg(
        tickets_closed=('no_of_tickets_received','nunique'), first_response_time=('first_response_time','mean'), resolution_time=('resolution_time','mean')
        ).sort_values(by=['year','month_number'],ascending=[True,True])[['month_year', 'tickets_closed', 'first_response_time', 'resolution_time']].transpose().reset_index()

    ctt_created_month_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_created_month_final.shape[0])]
    ctt_closed_month_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_closed_month_final.shape[0])]
    
    print(ctt_created_month_final)
    print(ctt_closed_month_final)


    

    ctt_created_quarter_final = ctt_created_month_df.copy().groupby(
        by=['year', 'quarter_number', 'quarter_year'], as_index=False
    ).agg(
        tickets_created=('no_of_tickets_received', 'nunique'),
        tickets_open=('open_tickets', 'nunique'),
        resolution_time_current=('resolution_time', 'mean'),
        net_open_tickets=('quarter_compare', lambda x: (x == 'different quarter').sum())  # Count tickets where quarter_compare is 'same quarter'
    ).sort_values(by=['year', 'quarter_number'], ascending=[True, True])[['quarter_year', 'tickets_created', 'net_open_tickets', 'tickets_open', 'resolution_time_current']].transpose().reset_index()

    # # Compute net open tickets
    # ctt_created_quarter_final['net_open_tickets'] = ctt_created_quarter_final['tickets_created'] - ctt_created_quarter_final['tickets_closed_same_quarter']

    # # Select relevant columns for final output
    # ctt_created_quarter_final = ctt_created_quarter_final[
    # ['quarter_year', 'tickets_created', 'tickets_closed_same_quarter', 'net_open_tickets', 'tickets_open', 'resolution_time_current']
    # ].transpose().reset_index()

    ctt_closed_quarter_final=ctt_closed_month_df.copy().groupby(by=['year','quarter_number','quarter_year'],as_index=False).agg(
        tickets_closed=('no_of_tickets_received','nunique'), first_response_time=('first_response_time','mean'), resolution_time=('resolution_time','mean')
        ).sort_values(by=['year','quarter_number'],ascending=[True,True])[['quarter_year', 'tickets_closed', 'first_response_time', 'resolution_time']].transpose().reset_index()

    ctt_created_quarter_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_created_quarter_final.shape[0])]
    ctt_closed_quarter_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_closed_quarter_final.shape[0])]
    



    ctt_created_year_final = ctt_created_month_df.copy().groupby(
    by=['year'], as_index=False
    ).agg(
        tickets_created=('no_of_tickets_received', 'nunique'),
        tickets_open=('open_tickets', 'nunique'),
        resolution_time_current=('resolution_time', 'mean'),
        net_open_tickets=('year_compare', lambda x: (x == 'different year').sum())  # Count tickets where year_compare is 'same year'
    ).sort_values(by=['year'], ascending=[True])[['year', 'tickets_created', 'net_open_tickets', 'tickets_open', 'resolution_time_current']].transpose().reset_index()

    # # Compute net open tickets
    # ctt_created_year_final['net_open_tickets'] = ctt_created_year_final['tickets_created'] - ctt_created_year_final['tickets_closed_same_year']

    # # Select relevant columns for final output
    # ctt_created_year_final = ctt_created_year_final[
    # ['year', 'tickets_created', 'tickets_closed_same_year', 'net_open_tickets', 'tickets_open', 'resolution_time_current']
    # ].transpose().reset_index()

    ctt_closed_year_final=ctt_closed_month_df.copy().groupby(by=['year'],as_index=False).agg(
        tickets_closed=('no_of_tickets_received','nunique'), first_response_time=('first_response_time','mean'), resolution_time=('resolution_time','mean')
        ).sort_values(by=['year'],ascending=[True])[['year', 'tickets_closed', 'first_response_time', 'resolution_time']].transpose().reset_index()

    ctt_created_year_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_created_year_final.shape[0])]
    ctt_closed_year_final.loc[:,'blanlk_column']=[np.nan for i in range(0,ctt_closed_year_final.shape[0])]
    
    print(ctt_created_year_final)
    print(ctt_closed_year_final)

    ctt_created_quarter_final['index']=ctt_created_quarter_final['index'].str.replace('quarter_year','Metrics/Timeline')
    ctt_closed_quarter_final['index']=ctt_closed_quarter_final['index'].str.replace('quarter_year','Metrics/Timeline')

    ctt_created_month_final['index']=ctt_created_month_final['index'].str.replace('month_year','Metrics/Timeline')
    ctt_closed_month_final['index']=ctt_closed_month_final['index'].str.replace('month_year','Metrics/Timeline')

    ctt_created_week_final['index']=ctt_created_week_final['index'].str.replace('week','Metrics/Timeline')
    ctt_closed_week_final['index']=ctt_closed_week_final['index'].str.replace('week','Metrics/Timeline')

    ctt_created_year_final['index']=ctt_created_year_final['index'].str.replace('year','Metrics/Timeline')
    ctt_closed_year_final['index']=ctt_closed_year_final['index'].str.replace('year','Metrics/Timeline')

    ctt_created_consolidated=ctt_created_week_final.merge(ctt_created_quarter_final,on='index').merge(ctt_created_month_final,on='index').merge(ctt_created_year_final,on='index',suffixes=('_a','_b'))
    ctt_closed_consolidated=ctt_closed_week_final.merge(ctt_closed_quarter_final,on='index').merge(ctt_closed_month_final,on='index').merge(ctt_closed_year_final,on='index',suffixes=('_a','_b'))

    ctt_created_consolidated=ctt_created_consolidated.rename(columns=ctt_created_consolidated.iloc[0,:]).iloc[1:,]
    ctt_closed_consolidated=ctt_closed_consolidated.rename(columns=ctt_closed_consolidated.iloc[0,:]).iloc[1:,]

    ctt_created_consolidated=ctt_created_consolidated.set_index('Metrics/Timeline')
    ctt_closed_consolidated=ctt_closed_consolidated.set_index('Metrics/Timeline')

    
    # ctt_created_consolidated.loc[' ',:]=[np.nan for i in range(0,ctt_created_consolidated.shape[1])]
    ctt_closed_consolidated.loc[' ',:]=[np.nan for i in range(0,ctt_closed_consolidated.shape[1])]
    
    print(ctt_created_consolidated)


    ctt_consolidated = pd.concat([ctt_created_consolidated, ctt_closed_consolidated]).round(2)
    # air_consolidated


    ctt_consolidated.index = ctt_consolidated.index.str.replace('tickets_created', 'Tickets Created').str.replace('tickets_open', 'Tickets Open').str.replace('tickets_closed', 'Tickets Closed').str.replace('first_response_time', 'First Response Time (Avg)').str.replace('resolution_time', "Resolution Time (Avg)").str.replace('Resolution Time (Avg)_current', "Resolution Time (Avg) Current Time Frame").str.replace('net_open_tickets', "Net Open Tickets")
    ctt_consolidated.round(2)
    
    return ctt_consolidated 
  
current_quarter='Q'+str(pd.Timestamp('today').quarter) + "'" + str(pd.to_datetime('today').year)[2:]
same_quarter_last_year='Q'+str(pd.Timestamp('today').quarter) + "'" + str(pd.to_datetime('today').year-1)[2:]
current_month=str(pd.Timestamp('today').month_name())[:3] + "'" + str(pd.to_datetime('today').year)[2:]
same_month_last_year=str(pd.Timestamp('today').month_name())[:3] + "'" + str(pd.to_datetime('today').year-1)[2:]

def new_metrics(df):
    non_zero_mask_wow=df.iloc[:,2]!=0
    
    df.loc[non_zero_mask_wow, 'WoW%'] = (df.loc[non_zero_mask_wow, df.columns[3]] - df.loc[non_zero_mask_wow, df.columns[2]]) / df.loc[non_zero_mask_wow, df.columns[2]]
    
    #current_month=str(pd.Timestamp('today').month_name())[:3] + "'" + str(pd.to_datetime('today').year)[2:]
    current_month_index=df.columns.get_loc(current_month)
    non_zero_mask_mom=df.iloc[:,current_month_index-1]!=0
    df.loc[non_zero_mask_mom, 'MoM%'] = (df.loc[non_zero_mask_mom, df.columns[current_month_index]] - df.loc[non_zero_mask_mom, df.columns[current_month_index - 1]]) / df.loc[non_zero_mask_mom, df.columns[current_month_index - 1]]
    
    
    #current_quarter='Q'+str(pd.Timestamp('today').quarter) + "'" + str(pd.to_datetime('today').year)[2:]
    current_quarter_index=df.columns.get_loc(current_quarter)
    non_zero_mask_qoq=df.iloc[:,current_quarter_index-1]!=0
    df.loc[non_zero_mask_qoq, 'QoQ%'] = (df.loc[non_zero_mask_qoq, df.columns[current_quarter_index]] - df.loc[non_zero_mask_qoq, df.columns[current_quarter_index - 1]]) / df.loc[non_zero_mask_qoq, df.columns[current_quarter_index - 1]]
    
    non_zero_mask_yoy=df.loc[:,pd.Timestamp('today').year-1]!=0
    df.loc[non_zero_mask_yoy,'YoY%']=(df.loc[non_zero_mask_yoy,pd.Timestamp('today').year]-df.loc[non_zero_mask_yoy,pd.Timestamp('today').year-1])/df.loc[non_zero_mask_yoy,pd.Timestamp('today').year-1]
    df['YoY%']

    #current_quarter='Q'+str(pd.Timestamp('today').quarter) + "'" + str(pd.to_datetime('today').year)[2:]
    #same_quarter_last_year='Q'+str(pd.Timestamp('today').quarter) + "'" + str(pd.to_datetime('today').year-1)[2:]
    non_zero_mask_sqly=df.loc[:,same_quarter_last_year]!=0
    df.loc[non_zero_mask_sqly, current_quarter + ' vs '+ same_quarter_last_year + '%']=(df.loc[non_zero_mask_sqly,current_quarter]-df.loc[non_zero_mask_sqly,same_quarter_last_year])/df.loc[non_zero_mask_sqly,same_quarter_last_year]

    #same_month_last_year=str(pd.Timestamp('today').month_name())[:3] + "'" + str(pd.to_datetime('today').year-1)[2:]
    non_zero_mask_smly=df.loc[:,same_month_last_year]!=0
    df.loc[non_zero_mask_smly,current_month + ' vs '+ same_month_last_year + '%']=(df.loc[non_zero_mask_smly,current_month]-df.loc[non_zero_mask_smly,same_month_last_year])/df.loc[non_zero_mask_smly,same_month_last_year]
    return df.round(2)

def rearrange_columns(df):

    columns=list(df.columns[:4])
    columns.append("WoW%")
    columns= columns+list(df.columns[4:9])
    columns.append(df.columns[4])
    columns= columns+list(df.columns[9:13])
    columns.append("QoQ%")
    columns.append(current_quarter + ' vs '+ same_quarter_last_year + '%')
    columns= columns+list(df.columns[13:26])
    columns.append(df.columns[4])
    columns= columns+list(df.columns[26:38])
    columns.append("MoM%")
    columns.append(current_month + ' vs '+ same_month_last_year + '%')
    columns= columns+list(df.columns[38:41])
    columns.append("YoY%")
    return df[columns]



writer = pd.ExcelWriter(OUTPUT_PATH,engine='xlsxwriter')
workbook=writer.book
def df_to_excel(df, tab_name):
    worksheet = workbook.add_worksheet(tab_name)
    writer.sheets[tab_name] = worksheet
    df.to_excel(writer, sheet_name=tab_name, startrow=1, startcol=0, index=True, header=True, freeze_panes=(2,1))
    
    # Define formats
    percent_format = workbook.add_format({'num_format': '0.0%', 'align': 'center'})
    headers_format = workbook.add_format({'bold': True, 'font_color': 'white', 'align': 'center', 'bg_color': '#000000', 'font_size': 25})
    num_format = workbook.add_format({'num_format': '0'})  # No decimal places

    vcenter = workbook.add_format({'bold': True, 'align': 'center_across'})
    white_bg = workbook.add_format({'bg_color': 'white'})
    
    
    # Rest of the formatting remains the same
    worksheet.conditional_format('A2:BA2', {'type': 'no_blanks', 'format': headers_format})
    worksheet.conditional_format('A2:A300', {'type': 'no_blanks', 'format': headers_format})
    worksheet.conditional_format('C2:C300', {'type': 'blanks', 'format': vcenter})
    worksheet.conditional_format('A2:A300', {
        'type': 'cell',
        'criteria': '=',
        'value': ' ',
        'format': white_bg
    })
    worksheet.set_row(1, 15)
    
    # Percentage columns formatting
    percent_columns = ['F', 'Q', 'R', 'AS', 'AT', 'AX']
    for col in percent_columns:
        worksheet.conditional_format(f'{col}3:{col}300', {'type': 'no_blanks', 'format': percent_format})
        worksheet.conditional_format(f'{col}3:{col}300', {
            'type': 'data_bar',
            'min_value': -1,
            'max_value': 1,
            'data_bar_2010': True,
            'bar_color': '#FF0000',
            'bar_negative_color': '#63C384'
        })

    # Apply number format to numerical columns
    numeric_columns = ['B', 'C', 'D', 'E', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 
                       'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AU', 'AV', 'AW']


    for col in numeric_columns:
        for row in [6, 8, 9]:  # Apply formatting to rows 5, 7, and 8
            worksheet.conditional_format(f'{col}{row}:{col}{row}', {'type': 'no_blanks', 'format': num_format})
  # Apply two decimal places

    worksheet.autofit()
    # Manually setting column widths
    for col in range(1, 52):
        worksheet.set_column(col, col, 12)
    
    print('done')

########################################################### CATEGORY WISE EXCEL EXPORT ###################################################
def df_to_excel_category(df, tab_name):
    worksheet = workbook.add_worksheet(tab_name)
    writer.sheets[tab_name] = worksheet
    df.to_excel(writer, sheet_name=tab_name, startrow=1, startcol=0, index=True, header=True, freeze_panes=(2,2))
    
    # Define formats
    percent_format = workbook.add_format({'num_format': '0.0%', 'align': 'center'})
    headers_format = workbook.add_format({'bold': True, 'font_color': 'white', 'align': 'center', 'bg_color': '#000000', 'font_size': 25})
    num_format = workbook.add_format({'num_format': '0'})  # No decimal places

    vcenter = workbook.add_format({'bold': True, 'align': 'center_across'})
    white_bg = workbook.add_format({'bg_color': 'white'})
    
    
    # Rest of the formatting remains the same
    worksheet.conditional_format('A2:BA2', {'type': 'no_blanks', 'format': headers_format})
    worksheet.conditional_format('B2:B300', {'type': 'no_blanks', 'format': headers_format})
    worksheet.conditional_format('C2:C300', {'type': 'blanks', 'format': vcenter})
    worksheet.conditional_format('A2:A300', {
        'type': 'cell',
        'criteria': '=',
        'value': ' ',
        'format': white_bg
    })
    worksheet.set_row(1, 15)
    
    # Percentage columns formatting
    percent_columns = ['G', 'R', 'S', 'AT', 'AU', 'AY']
    for col in percent_columns:
        worksheet.conditional_format(f'{col}3:{col}300', {'type': 'no_blanks', 'format': percent_format})
        worksheet.conditional_format(f'{col}3:{col}300', {
            'type': 'data_bar',
            'min_value': -1,
            'max_value': 1,
            'data_bar_2010': True,
            'bar_color': '#FF0000',
            'bar_negative_color': '#63C384'
        })

    # Apply number format to numerical columns
    numeric_columns = ['C', 'D', 'E', 'F', 'I', 'J', 'K', 'L', 'N', 'O', 'P', 'Q', 'U', 'V', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 
                       'AD', 'AE', 'AF', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AW', 'AX']


    for col in numeric_columns:
        worksheet.conditional_format(f'{col}1:{col}1048576', {'type': 'no_blanks', 'format': num_format})

  # Apply two decimal places

    worksheet.autofit()
    # Manually setting column widths
    for col in range(2, 52):
        worksheet.set_column(col, col, 12)
    
    print('done')

consolidated_wbr=generate_df(category=category)
consolidated=new_metrics(df=consolidated_wbr)
consolidated=rearrange_columns(df=consolidated)

categories = [
    "Air - Delivery", "Air- Export clearance", "Air  - Import clearance", "Air - Pre-booking",
    "Amazon Query", "B2B Query", "Concern not shared", "CTT 1", "Disputes", "Documentation",
    "Exports", "Finance", "Finance - Air", "Finance - Ocean", "General Query", "Imports",
    "Ocean - Delivery", "Ocean - Export clearance", "Ocean - Import Clearance",
    "Ocean - Pre-booking", "Pickup", "SEND Query"
]

# Generate dataframes for each category and store them in a dictionary
category_dfs = {f"{cat}": generate_df(category=[cat]) for cat in categories}

# Concatenate all dataframes with their respective keys
category_concat = pd.concat(category_dfs.values(), keys=category_dfs.keys())
consolidated_category=new_metrics(df=category_concat)
consolidated_category=rearrange_columns(df=consolidated_category)


def_data = {
    'Metric': [ 'Tickets created',
        'Net Open tickets',
        'Tickets open',
        'Resolution time current time frame',
        'Tickets closed',
        'First response time',
        'Resolution time (avg)'
    ],
    'Definition': [
        'tickets which had creation date in that particular week',
        'tickets created in that particular week which were open in that week',
        'tickets created in that particular week which are still open',
        'average of difference between closure date and creation date for tickets closed and created in the same week',
        'no. of tickets which had closure date in the particular week',
        'average of difference between first response date and ticket creation date',
        'average of difference between closure date and creation date for tickets which had closure date in the particular week'
    ]
}

# Convert the dictionary to a DataFrame
df_definitions = pd.DataFrame(def_data)


df_definitions.to_excel(writer, sheet_name='Definitions', index=False)
accessorials_data_copy.to_excel(writer, sheet_name='Raw Data', index=False)
workbook  = writer.book
worksheet = writer.sheets['Raw Data']
for col in range(0, 30):  # Columns C (index 2) to AZ (index 51)
        worksheet.set_column(col, col, 25)

worksheet = writer.sheets['Definitions']
for col in range(0, 30):  # Columns C (index 2) to AZ (index 51)
        worksheet.set_column(col, col, 30)

df_to_excel(df=consolidated,tab_name='Consolidated')
df_to_excel_category(df=consolidated_category,tab_name='Consolidated Category')
# df_to_excel(df=air,tab_name='Air Metrics')
# df_to_excel(df=ocean,tab_name='Ocean Metrics')


writer.close()
# Send the email with the report as an attachment
send_email(OUTPUT_PATH)

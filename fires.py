import cartopy.crs as ccrs
import numpy as np
from pyproj import Proj
import xarray
import matplotlib.pyplot as plt
import s3fs
import metpy
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas as pd
import lxml
from textwrap import wrap

import multiprocessing as mp

cpu_count = mp.cpu_count() 




fs = s3fs.S3FileSystem(anon=True)

#getting a list of files in directory
file = fs.ls('s3://noaa-goes17/ABI-L2-MCMIPC/2020/')

#Here I am grabbing a table from wikipedia that includes the start and containment date for wildfires in California
#one issue discovered was that one fire was still in progress, so I deleated it

#This data will be used later on to append a title to our images


#https://stackoverflow.com/questions/50355577/scraping-wikipedia-tables-with-python-selectively

import requests
from bs4 import BeautifulSoup

wiki_table = pd.DataFrame(columns = ['Name', 'County', 'Acres', 'Start', 'End', 'Notes'])

URL = "https://en.wikipedia.org/wiki/2020_California_wildfires"

res = requests.get(URL).text
soup = BeautifulSoup(res)
for items in soup.find('table', class_="wikitable").find_all('tr')[1::1]:
    data = items.find_all(['th','td'])
    try:
        Name = data[0].text.rstrip()
        County = data[1].text.rstrip()
        Acres = data[2].text.rstrip()
        Start = data[3].text.rstrip() + " 2020"
        End = data[4].text.rstrip() + " 2020"
        Notes = data[5].text.rstrip()
    except IndexError:pass
    
    wiki_dict = {'Name' : Name, 'County': County, 'Acres' : Acres, 'Start' : Start, 'End' : End, 'Notes' : Notes}
    wiki_table = wiki_table.append(wiki_dict, ignore_index = True)
    


wiki_table = wiki_table[wiki_table['Name'] != 'Dolan']

# convert to day of year
wiki_table['Start'] = pd.to_datetime(wiki_table['Start']).dt.dayofyear
wiki_table['End'] = pd.to_datetime(wiki_table['End']).dt.dayofyear

start_list = wiki_table['Start'].tolist()
end_list = wiki_table['End'].tolist()

days_needed = []
for i, j in zip(start_list, end_list):
    for day in range(i, j):
        days_needed.append(day)
    

#drop doubles
possible_days = set(days_needed)


def multi(day):


    file = fs.glob(f's3://noaa-goes17/ABI-L2-MCMIPC/2020/{day}/03/')[0] #7:00 PM PST

    item = str(file)
    lst = item.split("/")
    name = lst[5]

    path = "X:/fires/" + name

    fs.download(file, path)

    open_file = xarray.open_dataset(path)
    
    
################################################################################################

#code from https://github.com/blaylockbk/pyBKB_v3/blob/master/BB_GOES/mapping_GOES16_FireTemperature.ipynb

    # Scan's start time, converted to datetime object
    scan_start = datetime.strptime(open_file.time_coverage_start, '%Y-%m-%dT%H:%M:%S.%fZ')

    # Scan's end time, converted to datetime object
    scan_end = datetime.strptime(open_file.time_coverage_end, '%Y-%m-%dT%H:%M:%S.%fZ')

    # File creation time, convert to datetime object
    file_created = datetime.strptime(open_file.date_created, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    midpoint = str(open_file['t'].data)[:-8]
    scan_mid = datetime.strptime(midpoint, '%Y-%m-%dT%H:%M:%S.%f')


    # Load the three channels into appropriate R, G, and B variables
    R = open_file['CMI_C07'].data
    G = open_file['CMI_C06'].data
    B = open_file['CMI_C05'].data

    # Normalize each channel by the appropriate range of values  e.g. R = (R-minimum)/(maximum-minimum)
    R = (R-273)/(333-273)
    G = (G-0)/(1-0)
    B = (B-0)/(0.75-0)

    # Apply range limits for each channel. RGB values must be between 0 and 1
    R = np.clip(R, 0, 1)
    G = np.clip(G, 0, 1)
    B = np.clip(B, 0, 1)

    # Apply the gamma correction to Red channel.
    #   corrected_value = value^(1/gamma)
    gamma = 0.4
    R = np.power(R, 1/gamma)

    # The final RGB array :)
    RGB = np.dstack([R, G, B])

    # Satellite height
    sat_h = open_file['goes_imager_projection'].perspective_point_height

    # Satellite longitude
    sat_lon = open_file['goes_imager_projection'].longitude_of_projection_origin

    # Satellite sweep
    sat_sweep = open_file['goes_imager_projection'].sweep_angle_axis

    # The projection x and y coordinates equals the scanning angle (in radians) multiplied by the satellite height
    # See details here: https://proj4.org/operations/projections/geos.html?highlight=geostationary
    x = open_file['x'][:] * sat_h
    y = open_file['y'][:] * sat_h
    
################################################################################################



################################################################################################

#code from https://unidata.github.io/python-gallery/examples/mapping_GOES16_TrueColor.html


    # We'll use the `CMI_C02` variable as a 'hook' to get the CF metadata.
    dat = open_file.metpy.parse_cf('CMI_C02')

    geos = dat.metpy.cartopy_crs

    # We also need the x (north/south) and y (east/west) axis sweep of the ABI data
    x = dat.x
    y = dat.y

    fig = plt.figure(figsize=(8, 8))

    pc = ccrs.PlateCarree()

    ax = fig.add_subplot(1, 1, 1, projection=pc)
    ax.set_extent([-125, -114, 32, 42.5], crs=pc)

    ax.imshow(RGB, origin='upper',
              extent=(x.min(), x.max(), y.min(), y.max()),
              transform=geos,
              interpolation='none')

    ax.coastlines(resolution='50m', color='white', linewidth=1)
    ax.add_feature(ccrs.cartopy.feature.STATES, edgecolor = 'white')
    
################################################################################################

    #add title to show name of fire and date
    # Name of fire
    
    names = wiki_table[(wiki_table['Start'] <= day) & (wiki_table['End'] >= day)]['Name'].tolist()
    title = ', '.join(names)

    #took a while to figure this title out
    time = scan_start.strftime('%B %d %Y')
    plt.title("\n".join(wrap(f'Fire(s): {title}',60))  + f'\nDate: {time}')
    
    
    #("\n".join(wrap(f'Fires(s): {title}')))
    
    #plt.title(f'Fires(s): {title}', loc='left')
    #plt.title('{}'.format(scan_start.strftime('%d %B %Y %H:%M UTC ')), loc='right')
    
    plt.savefig(f'X:/geos17/{day}.png')
    plt.close() #got warning, this lowers ram usage
        

            
if __name__ == '__main__':

    
    pool = mp.Pool(cpu_count)
    for _ in tqdm(pool.imap_unordered(multi, [day for day in possible_days]), total = len(possible_days)):
        pass
    #results = pool.map(multi, [patient for patient in PATIENTS]) #here we call the funtion and the list we want to pass
    

    pool.close()
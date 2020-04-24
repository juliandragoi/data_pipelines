
import shutil
import datetime

now = datetime.datetime.now()

shutil.move('/home/pi/Desktop/GSDcoExtended.json', '/home/pidrop/pi3/GSDcoExtended' + now.strftime("%Y-%m-%d") + '.json')

from datetime import datetime, timedelta

idag = datetime.now()
igår = idag - timedelta(days=1)
år = idag.year
måned = idag.month
dag = idag.day
if måned <= 7:
    termin = "VÅR"
else:
    termin = "HØST"
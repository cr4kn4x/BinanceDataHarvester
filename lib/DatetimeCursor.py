from datetime import datetime

class Cursor(datetime):
    def get_month(self):
        month = str(self.month)
        if len(month) == 1:
            month = f"0{month}"
        return month
    
    def get_day(self): 
        day = str(self.day)
        if len(day) == 1: 
            day = f"0{day}"
        return day
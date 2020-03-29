import datetime, time
from utils import Crawl, SaveToDB, plotting, dump_db




# Main
def main():
    start = time.time()

    # Crawl and save data
    days = int(input("Enter the number of desired past days for retrieving data: "))
    SaveToDB.save_data(days)    

    # dump_db()


    end = time.time()
    print("time elapsed:", end-start)



# Run main funtion
if __name__ == '__main__':
    main()




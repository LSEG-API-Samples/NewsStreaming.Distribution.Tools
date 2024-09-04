## NewsStreaming.Distribution.Tools

### RDP News Streaming Tool description

1. Get all subscriptions 

   1.1 news-stories
       
       - python newsMessages.py -ct news-stories -g

   1.2 news-headlines

       - python newsMessages.py -ct news-headlines -g

 
2. Get specific subscriptions

   2.1 news-stories

       - python newsMessages.py -ct news-stories -g -s <subscriptionId>

   2.2 news-headlines

       - python newsMessages.py -ct news-headlines -g -s <subscriptionId>

3. create a new subscription only

   3.1 news-stories

       - python newsMessages.py -ct news-stories -c

   3.2 news-headlines

       - python newsMessages.py -ct news-headlines -c 

4. create a new subscription and then keep polling queue

   4.1 news-stories

       - python newsMessages.py -ct news-stories -c -p

   4.2 news-headlines

       - python newsMessages.py -ct news-headlines -c -p
		
5. [Demo Mode] create a new subscription and then keep polling queue but delete subscription after exit application

   5.1 news-stories

       - python newsMessages.py -ct news-stories -c -p -d

   5.2 news-headlines
        
       - python newsMessages.py -ct news-headlines -c -p -d

6. poll message queue from existing subscription

   6.1 news-stories

       - python newsMessages.py -ct news-stories -p -s <subscriptionId>

   6.2 news-headlines

       - python newsMessages.py -ct news-headlines -p -s <subscriptionId>

7. poll message queue by using default subscription and create new subscription if it does not exist
   
   7.1 news-stories

       - python newsMessages.py -ct news-stories -p

   7.2 news-headlines
        
       - python newsMessages.py -ct news-headlines -p

8. Delete all subscriptions

   8.1 news-stories

       - python newsMessages.py -ct news-stories -r

   8.2 news-headlines
       
       - python newsMessages.py -ct news-headlines -r
   
9. Delete specific subscription

   9.1 news-stories
        
        - python newsMessages.py -ct news-stories -r -s <subscriptionId>

   9.2 news-headlines
          
        - python newsMessages.py -ct news-headlines -r -s <subscriptionId>
		
10. Get all subscription queue statistics

   10.1 news-stories
         
        - python newsMessages.py -ct news-stories -q

   10.2 news-headlines

        - python newsMessages.py -ct news-headlines -q

11. Get specific subscription queue statistics

   11.1 news-stories
         
        - python newsMessages.py -ct news-stories -q -s <subscriptionId>

   11.2 news-headlines
         
        - python newsMessages.py -ct news-headlines -q -s <subscriptionId>
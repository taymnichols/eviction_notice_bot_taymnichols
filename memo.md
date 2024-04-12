Summary of process and progress in making this bot:
Building this bot was extremely frustrating and challenging for most of the time. I learned a lot about the process and then unlearned it all/found out I had learned nothing. 
I initially wanted to create a bot that told me the number of evictions per ward, per main address, and the new evictions added. However, I found that the more I tried to make this
bot do, the more muddy things became. I think the biggest problems I ran into came when I tried to modify the data to add a city and break the address up into street address and apartment number.
In the end I decided to keep my bot very simple because I could barely get the simple version to run. I also struggled to get the bot to tell me the latest date (it had trouble converting the eviction date column
into a date) and to tell me how many new rows I added. I think the biggest lesson I learned here is that ChatGPT can't just write my whole code for me. I feel like I have a much better grasp on what is happening
in my code for the most part, because I had to understand each piece to figure out what wasn't working. Since modifying the data to map to ward and identify the root address was so challenging, my bot and slack message
ended up being very simple. I think the output is pretty bare bones but I am happy it finally runs. I felt that keeping things simple was my best bet at this point. I think I was overly focused on what I wanted the data
to look like at the end rather than making the bot part which cost me time on this project.

1. Storing the data: I originally set this up to be stored in a Datasette database. I got rid of that so that my files and code would be clean. I think I would like this to be stored in an app similar to what we did for the
Baltimore overdose calls, with a sortable table and a map. I think having a heatmap would be good and if possible, including wards on the map.
2. Accepting input from users is an interesting idea. I think keeping it simple and just having an email address where people can reach someone would be good - I don't want people to think they can contact me for help with their
evictions, though. I also have some concerns about people's addresses being posted online in the first place but they aren't attached to names so it's probably okay.
3. I think the best schedule for updates is whenever there is new data available, since it happens semi-rarely.
4. A more useful verison of this bot would include some analysis on the areas of DC most vulnerable to evictions I think. I also would like to include Census data if possible, so we can look at factors such as racial and financial
makeup of neighborhoods most affected by evictions. I will need to hook my app up to the Census API and also an API to map my addresses to wards in DC. It would be good to have some visuals or text to give context for the app, 
although I think that might require some additional reporting. Maybe resources for people affected by evictions?

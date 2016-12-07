1. Checking the table

Query: select * from violations limit 20


2. Counting the number of violations every year

Query: select count(violation), year from violations group by violation, year order by year



3. Counting the number of violations for every month of a given year


Query: select count(violation), month, year from violations group by violation, month, year order by year, month


4. Total amount fined for violation every year

Query: select count(violation), sum(amount), year from violations group by violation, year order by year


5. Average amount fined for violation every year

Query: select count(violation), avg(amount), year from violations group by violation, year order by year


6. Amount fined for violations per precient in last 5 years

Query: select count(violation), sum(amount), precinct from violations group by violation,  precinct order by  precinct
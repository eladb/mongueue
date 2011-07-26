start mongod --port 60000 --dbpath ./tempdb
call node examples/stress.js
taskkill /im mongod.exe

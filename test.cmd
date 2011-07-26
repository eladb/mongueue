start mongod --port 60000 --dbpath ./tempdb
call node node_modules/vows/bin/vows test/* --spec %1 %2 %3 %4 %5 %6
taskkill /im mongod.exe

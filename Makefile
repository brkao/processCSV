all:
	make clean
	pip3 install --target ./package cassandra-driver
	(cd ./package && zip -r ../deployment-package.zip .)
	ls
	zip -g ./deployment-package.zip processCSV.py

clean:
	rm -rf ./package
	rm -rf *.zip

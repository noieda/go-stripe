SHELL=cmd
# STRIPE_SECRET=sk_test_51LBLBQBWsuY0cZrm45b3CJSMRKO7gG6nmnf49R5VS84uPkYEWP8PhL7cfQPDyJwEPshwkfBQNFuXycINnXAg8wMK00F0ljmk0X
# STRIPE_KEY=pk_test_51LBLBQBWsuY0cZrmuAEnUwehDWsPgkpsdNaf81TgcHrOCoJqVlpGGLXwEgNgY6IvrBJmlYXAoz9bOuasu6mRyw05004VJzsTzp
GOSTRIPE_PORT=4000
API_PORT=4001
DSN="root@(localhost:3306)/widgets?parseTime=true&tls=false"

include .env

## build: builds all binaries
build: clean build_front build_back
	@echo All binaries built!

## clean: cleans all binaries and runs go clean
clean:
	@echo Cleaning...
	@echo y | DEL /S dist
	@go clean
	@echo Cleaned and deleted binaries

## build_front: builds the front end
build_front:
	@echo Building front end...
	@go build -o dist/gostripe.exe ./cmd/web
	@echo Front end built!

## build_back: builds the back end
build_back:
	@echo Building back end...
	@go build -o dist/gostripe_api.exe ./cmd/api
	@echo Back end built!

## start: starts front and back end
start: start_front start_back

## start_front: starts the front end
start_front: build_front
	@echo Starting the front end...
	@set STRIPE_KEY=${STRIPE_KEY}&& set STRIPE_SECRET=${STRIPE_SECRET}&& start /B .\dist\gostripe.exe -dsn=${DSN}
	@echo Front end running!

## start_back: starts the back end
start_back: build_back
	@echo Starting the back end...
	@set STRIPE_KEY=${STRIPE_KEY}&& set STRIPE_SECRET=${STRIPE_SECRET}&& start /B .\dist\gostripe_api.exe -dsn=${DSN}
	@echo Back end running!

## stop: stops the front and back end
stop: stop_front stop_back
	@echo All applications stopped

## stop_front: stops the front end
stop_front:
	@echo Stopping the front end...
	@taskkill /IM gostripe.exe /F
	@echo Stopped front end

## stop_back: stops the back end
stop_back:
	@echo Stopping the back end...
	@taskkill /IM gostripe_api.exe /F
	@echo Stopped back end

## restart: stops and starts the front and back end
restart: stop_front stop_back start_front start_back

## restart_front: restart the front end
restart_front: stop_front start_front

## restart_back: restart the back end
restart_back: stop_back start_back
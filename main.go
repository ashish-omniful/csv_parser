package main

import (
	"awesomeProject3/models"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func main() {

	var err error
	fmt.Println("check")

	// connect to database
	dsn := "host=castor.db.elephantsql.com user=beghrxzo password=toYiNr2v9TSF4aUCaeh__hRLLyquHuhc dbname=beghrxzo port=5432 sslmode=disable"
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})

	// migrate the user tables
	err = DB.AutoMigrate(&models.User{})
	if err != nil {
		panic("cannot migrate")
	}

	// routes
	r := gin.Default()
	r.GET("/update_csv", handleUpdateCSV)

	// server
	err = r.Run(":8080")
	if err != nil {
		panic("connection not established at port 8080")
	}
}

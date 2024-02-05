package main

import (
	"awesomeProject3/csv"
	"awesomeProject3/models"
	"github.com/gin-gonic/gin"
	"net/http"
)

func handleUpdateCSV(ctx *gin.Context) {

	commonCSV, err := csv.NewCommonCSV(csv.WithBatchSize(2))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": "error while creating instance of commonCSV",
		})
		return
	}
	err = commonCSV.InitializeS3CSVReader(ctx)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": "error while initializing reader",
		})
		return
	}

	for {
		if commonCSV.IsEOF() {
			break
		}

		var user []models.User
		err = commonCSV.ParseNextBatch(&user)

		// database push code ..

		//err = processData(ctx, user)
		//if err != nil {
		//	ctx.JSON(http.StatusBadRequest, gin.H{
		//		"error": "error while processing data",
		//	})
		//	return
		//}

	}

	ctx.JSON(http.StatusOK, gin.H{
		"message": "success",
	})
	return
}

//func processData(ctx context.Context, users []models.User) (err error) {
//
//	for _, user := range users {
//
//		result := DB.Clauses(clause.OnConflict{
//			Columns:   []clause.Column{{Name: "phone_number"}},
//			DoUpdates: clause.AssignmentColumns([]string{"email", "country", "name"}),
//		}).Create(&user)
//
//		//result := DB.Create(&user)
//
//		if result.Error != nil {
//
//			err = result.Error]
//			return
//		}
//
//		fmt.Println("successfully updated data")
//	}
//
//	return
//}

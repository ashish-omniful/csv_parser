package models

type User struct {
	ID          uint64 `json:"id" gorm:"primaryKey"`
	Name        string `json:"name"`
	PhoneNumber string `json:"phone_number" gorm:"unique"`
	Email       string `json:"email"`
	Country     string `json:"country"`
}

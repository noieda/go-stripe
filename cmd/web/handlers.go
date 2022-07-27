package main

import (
	"go-stripe/internal/cards"
	"go-stripe/internal/models"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// display home
func (app *application) Home(w http.ResponseWriter, r *http.Request) {

	if err := app.renderTemplate(w, r, "home", &templateData{}); err != nil {
		app.errorLog.Println(err)
	}
}

// virtual terminal
func (app *application) VirtualTerminal(w http.ResponseWriter, r *http.Request) {

	// stringMap := make(map[string]string)
	// stringMap["publishable_key"] = app.config.stripe.key

	// fmt.Println(app.config)
	// fmt.Println(app.config.stripe.key)

	// fmt.Println(stringMap["publishable_key"])

	if err := app.renderTemplate(w, r, "terminal", &templateData{}, "stripe-js"); err != nil {
		app.errorLog.Println(err)
	}
}

type TransactionData struct {
	FirstName       string
	LastName        string
	Email           string
	PaymentIntentID string
	PaymentMethodID string
	PaymentAmount   int
	PaymentCurrency string
	LastFour        string
	ExpiryMonth     int
	ExpiryYear      int
	BackReturnCode  string
}

// GetTransactionData get txn data from post and stripe
func (app *application) GetTransactionData(r *http.Request) (TransactionData, error) {

	var txnData TransactionData

	err := r.ParseForm()
	if err != nil {
		app.errorLog.Println((err))
		return txnData, err
	}

	firstName := r.Form.Get("first_name")
	lastName := r.Form.Get("last_name")
	email := r.Form.Get("cardholder_email")
	paymentIntent := r.Form.Get("payment_intent")
	paymentMethod := r.Form.Get("payment_method")
	paymentAmount := r.Form.Get("payment_amount")
	paymentCurrency := r.Form.Get("payment_currency")

	amount, _ := strconv.Atoi(paymentAmount)
	// widgetID, _ := strconv.Atoi(r.Form.Get("product_id"))
	card := cards.Card{
		Secret: app.config.stripe.secret,
		Key:    app.config.stripe.key,
	}

	pi, err := card.RetrievePaymentIntent(paymentIntent)
	if err != nil {
		app.errorLog.Println(err)
		return txnData, err
	}

	pm, err := card.GetPaymentMethod(paymentMethod)
	if err != nil {
		app.errorLog.Println(err)
		return txnData, err
	}

	lastFour := pm.Card.Last4
	expiryMonth := pm.Card.ExpMonth
	expiryYear := pm.Card.ExpYear

	txnData = TransactionData{
		FirstName:       firstName,
		LastName:        lastName,
		Email:           email,
		PaymentIntentID: paymentIntent,
		PaymentMethodID: paymentMethod,
		PaymentAmount:   amount,
		PaymentCurrency: paymentCurrency,
		LastFour:        lastFour,
		ExpiryMonth:     int(expiryMonth),
		ExpiryYear:      int(expiryYear),
		BackReturnCode:  pi.Charges.Data[0].ID,
	}

	return txnData, nil
}

func (app *application) PaymentSucceeded(w http.ResponseWriter, r *http.Request) {

	// err := r.ParseForm()
	// if err != nil {
	// 	app.errorLog.Println((err))
	// 	return
	// }

	// read posted data
	// firstName := r.Form.Get("first_name")
	// lastName := r.Form.Get("last_name")
	// email := r.Form.Get("cardholder_email")
	// paymentIntent := r.Form.Get("payment_intent")
	// paymentMethod := r.Form.Get("payment_method")
	// paymentAmount := r.Form.Get("payment_amount")
	// paymentCurrency := r.Form.Get("payment_currency")
	widgetID, _ := strconv.Atoi(r.Form.Get("product_id"))

	// card := cards.Card{
	// 	Secret: app.config.stripe.secret,
	// 	Key:    app.config.stripe.key,
	// }

	txnData, err := app.GetTransactionData(r)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	// pi, err := card.RetrievePaymentIntent(paymentIntent)
	// if err != nil {
	// 	app.errorLog.Println(err)
	// 	return
	// }

	// pm, err := card.GetPaymentMethod(paymentMethod)
	// if err != nil {
	// 	app.errorLog.Println(err)
	// 	return
	// }

	// create new customer
	customerID, err := app.SaveCustomer(txnData.FirstName, txnData.LastName, txnData.Email)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	app.infoLog.Println(customerID)

	// create new order

	// create new transaction
	txn := models.Transaction{
		Amount:              txnData.PaymentAmount,
		Currency:            txnData.PaymentCurrency,
		LastFour:            txnData.LastFour,
		ExpiryMonth:         txnData.ExpiryMonth,
		ExpiryYear:          txnData.ExpiryYear,
		PaymentIntent:       txnData.PaymentIntentID,
		PaymentMethod:       txnData.PaymentMethodID,
		BankReturnCode:      txnData.BackReturnCode,
		TransactionStatusID: 2,
	}

	txnID, err := app.SaveTransaction(txn)
	// fmt.Println("lewat sana")
	if err != nil {
		// fmt.Println("lewat sini")

		app.errorLog.Println(err)
		return
	}

	order := models.Order{
		WidgetID:      widgetID,
		TransactionID: txnID,
		CustomerID:    customerID,
		StatusID:      1,
		Quantity:      1,
		Amount:        txnData.PaymentAmount,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	_, err = app.SaveOrder(order)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	// write this data to session and then redirect user to new page
	app.Session.Put(r.Context(), "receipt", txnData)
	http.Redirect(w, r, "/receipt", http.StatusSeeOther)

}

func (app *application) Receipt(w http.ResponseWriter, r *http.Request) {

	txn := app.Session.Get(r.Context(), "receipt").(TransactionData)
	app.Session.Remove(r.Context(), "receipt")

	data := make(map[string]interface{})

	data["txn"] = txn

	if err := app.renderTemplate(w, r, "receipt", &templateData{
		Data: data,
	}); err != nil {
		app.errorLog.Println(err)
	}

}

// SaveCustomer saves a customer and returns id
func (app *application) SaveCustomer(firstName, lastName, email string) (int, error) {
	customer := models.Customer{
		FirstName: firstName,
		LastName:  lastName,
		Email:     email,
	}

	id, err := app.DB.InsertCustomer(customer)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// SaveTransaction saves a txn and returns id
func (app *application) SaveTransaction(txn models.Transaction) (int, error) {

	id, err := app.DB.InsertTransaction(txn)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// SaveOrder saves a transaction and returns id
func (app *application) SaveOrder(order models.Order) (int, error) {

	id, err := app.DB.InsertOrder(order)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// ChargeOnce displays page for buy widgets once
func (app *application) ChargeOnce(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")
	widgetID, _ := strconv.Atoi(id)

	widget, err := app.DB.GetWidget(widgetID)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	data := make(map[string]interface{})
	data["widget"] = widget

	if err := app.renderTemplate(w, r, "buy-once", &templateData{Data: data}, "stripe-js"); err != nil {
		app.errorLog.Println(err)
	}
}

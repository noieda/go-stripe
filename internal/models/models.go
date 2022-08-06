package models

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// DBModel is type for database connection values
type DBModel struct {
	DB *sql.DB
}

// Models is wrapper for all models
type Models struct {
	DB DBModel
}

// Widgets is type for all widgets
type Widget struct {
	ID             int       `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	InventoryLevel int       `json:"inventory_level"`
	Price          int       `json:"price"`
	Image          string    `json:"image"`
	IsRecurring    bool      `json:"is_recurring"`
	PlanID         string    `json:"plan_id"`
	CreatedAt      time.Time `json:"-"`
	UpdatedAt      time.Time `json:"-"`
}

// Order is type for all orders
type Order struct {
	ID            int         `json:"id"`
	WidgetID      int         `json:"widget_id"`
	TransactionID int         `json:"transaction_id"`
	CustomerID    int         `json:"customer_id"`
	StatusID      int         `json:"status_id"`
	Quantity      int         `json:"quantity"`
	Amount        int         `json:"amount"`
	CreatedAt     time.Time   `json:"-"`
	UpdatedAt     time.Time   `json:"-"`
	Widget        Widget      `json:"widget"`
	Transaction   Transaction `json:"transaction"`
	Customer      Customer    `json:"customer"`
}

// status is type for status
type Status struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// TransactionStatus
type TransactionStatus struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// Transaction
type Transaction struct {
	ID                  int       `json:"id"`
	Amount              int       `json:"amount"`
	Currency            string    `json:"currency"`
	LastFour            string    `json:"last_four"`
	PaymentIntent       string    `json:"payment_intent"`
	PaymentMethod       string    `json:"payment_method"`
	ExpiryMonth         int       `json:"expiry_month"`
	ExpiryYear          int       `json:"expiry_year"`
	BankReturnCode      string    `json:"bank_return_code"`
	TransactionStatusID int       `json:"transaction_status_id"`
	CreatedAt           time.Time `json:"-"`
	UpdatedAt           time.Time `json:"-"`
}

// user
type User struct {
	ID        int       `json:"id"`
	FirstName string    `json:"first_name"`
	LastName  string    `json:"last_name"`
	Email     string    `json:"email"`
	Password  string    `json:"password"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// customer
type Customer struct {
	ID        int       `json:"id"`
	FirstName string    `json:"first_name"`
	LastName  string    `json:"last_name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// NewModels returns a model with type database connection pool
func NewModels(db *sql.DB) Models {
	return Models{
		DB: DBModel{
			DB: db,
		},
	}
}

func (m *DBModel) GetWidget(id int) (Widget, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var widget Widget

	row := m.DB.QueryRowContext(ctx, `
	select 
		id, name, description, inventory_level, price, coalesce(image, ''), is_recurring, plan_id, created_at, updated_at
	from 
		widgets 
	where 
		id = ?`, id)
	err := row.Scan(
		&widget.ID,
		&widget.Name,
		&widget.Description,
		&widget.InventoryLevel,
		&widget.Price,
		&widget.Image,
		&widget.IsRecurring,
		&widget.PlanID,
		&widget.CreatedAt,
		&widget.UpdatedAt,
	)
	if err != nil {
		return widget, err
	}

	// fmt.Println("disini")
	return widget, nil
}

// InsertTransaction is function for insert a new txn and return its id
func (m *DBModel) InsertTransaction(txn Transaction) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stmt := `
		insert into transactions
			(amount, currency, last_four, bank_return_code, expiry_month, expiry_year, payment_intent, payment_method, transaction_status_id, created_at, updated_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := m.DB.ExecContext(ctx, stmt,
		txn.Amount,
		txn.Currency,
		txn.LastFour,
		txn.BankReturnCode,
		txn.ExpiryMonth,
		txn.ExpiryYear,
		txn.PaymentIntent,
		txn.PaymentMethod,
		txn.TransactionStatusID,
		time.Now(),
		time.Now(),
	)

	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

// InsertOrder is function for insert a new txn and return its id
func (m *DBModel) InsertOrder(order Order) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stmt := `
		insert into orders
			(widget_id, transaction_id, status_id, quantity, customer_id,
			amount, created_at, updated_at)
		values (?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := m.DB.ExecContext(ctx, stmt,
		order.WidgetID,
		order.TransactionID,
		order.StatusID,
		order.Quantity,
		order.CustomerID,
		order.Amount,
		time.Now(),
		time.Now(),
	)

	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

// InsertOrder is function for insert a new txn and return its id
func (m *DBModel) InsertCustomer(c Customer) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stmt := `
		insert into customers 
			(first_name, last_name, email ,created_at, updated_at)
		values (?, ?, ?, ?, ?)
	`

	result, err := m.DB.ExecContext(ctx, stmt,
		c.FirstName,
		c.LastName,
		c.Email,
		time.Now(),
		time.Now(),
	)

	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

// GetUserByEmail gets user by email address
func (m *DBModel) GetUserByEmail(email string) (User, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	email = strings.ToLower(email)
	var u User

	row := m.DB.QueryRowContext(ctx, `
		select
			id, first_name, last_name, email, password, created_at, updated_at
		from
			users
		where email = ?
	`, email)

	err := row.Scan(
		&u.ID,
		&u.FirstName,
		&u.LastName,
		&u.Email,
		&u.Password,
		&u.CreatedAt,
		&u.UpdatedAt,
	)
	if err != nil {
		return u, err
	}

	return u, nil

}

func (m *DBModel) Authenticate(email, password string) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var id int
	var hashedPassword string

	row := m.DB.QueryRowContext(ctx, "select id, password form users where email = ?", email)
	err := row.Scan(&id, &hashedPassword)
	if err != nil {
		return id, err
	}

	err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err == bcrypt.ErrMismatchedHashAndPassword {
		return 0, errors.New("incorrect password")
	} else if err != nil {
		return 0, err
	}

	return id, nil

}

func (m *DBModel) UpdatePasswordForUser(u User, hash string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stmt := `update users set password = ? where id = ?`
	_, err := m.DB.ExecContext(ctx, stmt,
		hash,
		u.ID)
	if err != nil {
		return err
	}

	return nil
}

func (m *DBModel) GetAllOrders(orderType string) ([]*Order, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var orders []*Order

	var orderOrsubs int
	if orderType == "order" {
		orderOrsubs = 0
	} else if orderType == "subscription" {
		orderOrsubs = 1
	} else {
		return nil, nil
	}

	query := `
	SELECT
		o.id, o.widget_id, o.transaction_id, o.customer_id,
		o.status_id, o.quantity, o.amount, o.created_at,
		o.updated_at, w.id, w.name, t.id, t.amount, t.currency,
		t.last_four, t.expiry_month, t.expiry_year,
		t.payment_intent, t.bank_return_code,
		c.id, c.first_name, c.last_name, c.email
	FROM
		orders as o
		LEFT JOIN widgets AS w ON (o.widget_id = w.id)
		LEFT JOIN transactions AS t ON (o.transaction_id = t.id)
		LEFT JOIN customers AS c ON (o.customer_id = c.id)
	WHERE
		w.is_recurring = ?
	ORDER BY
		o.created_at desc
	`

	rows, err := m.DB.QueryContext(ctx, query, orderOrsubs)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var o Order
		err = rows.Scan(
			&o.ID,
			&o.WidgetID,
			&o.TransactionID,
			&o.CustomerID,
			&o.StatusID,
			&o.Quantity,
			&o.Amount,
			&o.CreatedAt,
			&o.UpdatedAt,
			&o.Widget.ID,
			&o.Widget.Name,
			&o.Transaction.ID,
			&o.Transaction.Amount,
			&o.Transaction.Currency,
			&o.Transaction.LastFour,
			&o.Transaction.ExpiryMonth,
			&o.Transaction.ExpiryYear,
			&o.Transaction.PaymentIntent,
			&o.Transaction.BankReturnCode,
			&o.Customer.ID,
			&o.Customer.FirstName,
			&o.Customer.LastName,
			&o.Customer.Email,
		)

		if err != nil {
			return nil, err
		}

		orders = append(orders, &o)
	}

	return orders, nil
}

// GetAllOrdersPaginated returns a slice of subset of orders
func (m *DBModel) GetAllOrdersPaginated(orderType string, pageSize, page int) ([]*Order, int, int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	offset := (page - 1) * pageSize

	var orders []*Order

	var orderOrsubs int
	if orderType == "order" {
		orderOrsubs = 0
	} else if orderType == "subscription" {
		orderOrsubs = 1
	} else {
		return nil, 0, 0, nil
	}

	query := `
	SELECT
		o.id, o.widget_id, o.transaction_id, o.customer_id,
		o.status_id, o.quantity, o.amount, o.created_at,
		o.updated_at, w.id, w.name, t.id, t.amount, t.currency,
		t.last_four, t.expiry_month, t.expiry_year,
		t.payment_intent, t.bank_return_code,
		c.id, c.first_name, c.last_name, c.email
	FROM
		orders as o
		LEFT JOIN widgets AS w ON (o.widget_id = w.id)
		LEFT JOIN transactions AS t ON (o.transaction_id = t.id)
		LEFT JOIN customers AS c ON (o.customer_id = c.id)
	WHERE
		w.is_recurring = ?
	ORDER BY
		o.created_at desc
	LIMIT ?
	OFFSET ?
	`

	rows, err := m.DB.QueryContext(ctx, query, orderOrsubs, pageSize, offset)
	if err != nil {
		return nil, 0, 0, err
	}

	for rows.Next() {
		var o Order
		err = rows.Scan(
			&o.ID,
			&o.WidgetID,
			&o.TransactionID,
			&o.CustomerID,
			&o.StatusID,
			&o.Quantity,
			&o.Amount,
			&o.CreatedAt,
			&o.UpdatedAt,
			&o.Widget.ID,
			&o.Widget.Name,
			&o.Transaction.ID,
			&o.Transaction.Amount,
			&o.Transaction.Currency,
			&o.Transaction.LastFour,
			&o.Transaction.ExpiryMonth,
			&o.Transaction.ExpiryYear,
			&o.Transaction.PaymentIntent,
			&o.Transaction.BankReturnCode,
			&o.Customer.ID,
			&o.Customer.FirstName,
			&o.Customer.LastName,
			&o.Customer.Email,
		)

		if err != nil {
			return nil, 0, 0, err
		}

		orders = append(orders, &o)
	}

	query = `
		SELECT 
			count(o.id)
		FROM
			orders o
			LEFT JOIN widgets w ON (o.widget_id = w.id)
		WHERE
			w.is_recurring = ?
	`

	var totalRecords int
	countRow := m.DB.QueryRowContext(ctx, query, orderOrsubs)
	err = countRow.Scan(&totalRecords)
	if err != nil {
		return nil, 0, 0, err
	}

	lastPage := totalRecords / pageSize

	return orders, lastPage, totalRecords, nil
}

func (m *DBModel) GetOrderByID(id int) (Order, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var o Order

	query := `
	SELECT
		o.id, o.widget_id, o.transaction_id, o.customer_id,
		o.status_id, o.quantity, o.amount, o.created_at,
		o.updated_at, w.id, w.name, t.id, t.amount, t.currency,
		t.last_four, t.expiry_month, t.expiry_year,
		t.payment_intent, t.bank_return_code,
		c.id, c.first_name, c.last_name, c.email
	FROM
		orders as o
		LEFT JOIN widgets AS w ON (o.widget_id = w.id)
		LEFT JOIN transactions AS t ON (o.transaction_id = t.id)
		LEFT JOIN customers AS c ON (o.customer_id = c.id)
	WHERE
		o.id = ?
	`

	row := m.DB.QueryRowContext(ctx, query, id)

	err := row.Scan(
		&o.ID,
		&o.WidgetID,
		&o.TransactionID,
		&o.CustomerID,
		&o.StatusID,
		&o.Quantity,
		&o.Amount,
		&o.CreatedAt,
		&o.UpdatedAt,
		&o.Widget.ID,
		&o.Widget.Name,
		&o.Transaction.ID,
		&o.Transaction.Amount,
		&o.Transaction.Currency,
		&o.Transaction.LastFour,
		&o.Transaction.ExpiryMonth,
		&o.Transaction.ExpiryYear,
		&o.Transaction.PaymentIntent,
		&o.Transaction.BankReturnCode,
		&o.Customer.ID,
		&o.Customer.FirstName,
		&o.Customer.LastName,
		&o.Customer.Email,
	)

	if err != nil {
		return o, err
	}

	return o, nil
}

func (m *DBModel) UpdateOrderStatus(id, statusID int) error {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stmt := `
		update 
			orders
		set
			status_id = ?
		where
			id = ?
	`

	_, err := m.DB.ExecContext(ctx, stmt, statusID, id)
	if err != nil {
		return err
	}

	return nil
}

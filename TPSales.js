conn = new Mongo();

//"tp2" - Nome da base de dados utilizada.
db = conn.getDB("DSStore");

//use DSStore;

//Alterar string para Data nos campos SellDate
var sellStartDate = db.ProductDetails.find({ SellStartDate: { $ne: "NULL" } }, {}).toArray();

sellStartDate.forEach(
    function (doc) {
        doc.SellStartDate = new ISODate(doc.SellStartDate);
        db.ProductDetails.save(doc);
    });

var sellEndDate = db.ProductDetails.find({ SellEndDate: { $ne: "NULL" } }, {}).toArray();
sellEndDate.forEach(
    function (doc) {
        doc.SellEndDate = new ISODate(doc.SellEndDate);
        db.ProductDetails.save(doc);
    });

//-- Transformação 1 -- 
var n = db.SalesDetails.aggregate([{
    "$lookup":
        {
            "from": "ProductDetails",
            "localField": "ProductID",
            "foreignField": "ProductID", "as": "Product"
        }
},
{
    $unset: ["Product.ProductNumber", "Product.Color",
        "Product.ListPrice", "Product.SellStartDate",
        "Product.SellEndDate"]
}, { $unwind: "$Product" },
{
    "$project": {
        "_id": 1, "ReceiptID": 1, "OrderDate": { $toDate: "$OrderDate" },
        "Customer": 1, "CurrencyRateID": 1, "SubTotal": { $toDouble: "$SubTotal" },
        "TaxAmt": { $toDouble: "$TaxAmt" }, "Store": { "Id": "$Store", "Name": "$StoreName" },
        "ReceiptLine": { "Id": "$ReceiptLineID", "Quantity": { $toInt: "$Quantity" }, "Product": { _id: "$Product._id", "ProductID": "$Product.ProductID", "Name": "$Product.Name" }, "UnitPrice": { $toDouble: "$UnitPrice" }, "LineTotal": { $toDouble: "$LineTotal" } }
    }
}, {
    $group: {
        _id: "$ReceiptID", OrderDate: { $first: { $toDate: "$OrderDate" } }, Customer: { $first: "$Customer" },
        CurrencyRateID: { $first: "$CurrencyRateID" }, SubTotal: { $first: "$SubTotal" }, TaxAmt: { $first: "$TaxAmt" }, Store: { $first: "$Store" },
        ReceiptLines: { $push: "$ReceiptLine" }
    }
}]).toArray();

//Inserir novos dados numa nova coleção
n.forEach(function (doc) {
    db.salesDetails.insertOne(doc);
});

//Eliminar a antiga coleção
db.SalesDetails.drop();


//-- Transformação 2 -- 
//Agrupar atributos comuns do cliente
var cust = db.CustomerDetails.aggregate([{
    "$project": {
        "_id": 1, "Keys": {
            "Key": "$CustomerKey",
            "AlternateKey": "$CustomerAlternateKey"
        },
        "Title": 1, "Name": {
            "First": "$FirstName",
            "Middle": "$MiddleName", "Last": "$LastName"
        }, "BirthDate": { $toDate: "$BirthDate" },
        "MaritalStatus": 1, "Gender": 1, "EmailAddress": 1,
        "Education": 1, "Ocupation": 1, "Address":
            { "Line1": "$AddressLine1", "Line2": "$AddressLine2" },
        "Phone": 1, "DateFirstPurchase":
            { $toDate: "$DateFirstPurchase" }
    }
}]).toArray();

//Inserir novos dados numa nova coleção
cust.forEach(function (doc) {
    db.Customer.insertOne(doc);
});

//Eliminar a antiga coleção
db.CustomerDetails.drop();

//-- Transformação 3 -- 
//Embutir Costumer em salesDetails
var t = db.salesDetails.aggregate([{
    "$lookup": {
        "from": "Customer",
        "localField": "Customer", "foreignField": "Keys.Key",
        "as": "Customer"
    }
}, { $unwind: "$Customer" }, {
    $unset: ["Customer.Title", "Customer.MaritalStatus", "Customer.Gender", "Customer.Education", "Customer.BirthDate", 
    "Customer.DateFirstPurchase", "Customer.EmailAddress"
        , "Customer.Address"]
}]).toArray();

//Inserir novos dados numa nova coleção
t.forEach(function (doc) {
    db.SalesDetails.insertOne(doc);
});


//Eliminar a antiga coleção
db.salesDetails.drop();

//Alterar string para data
var StartDate = db.ProductListPriceHistory.find({ StartDate: { $ne: "NULL" } }, {}).toArray();

StartDate.forEach(
    function (doc) {
        doc.StartDate = new ISODate(doc.StartDate);
        db.ProductListPriceHistory.save(doc);
    });

var EndDate = db.ProductListPriceHistory.find({ EndDate: { $ne: "NULL" } }, {}).toArray();
EndDate.forEach(
    function (doc) {
        doc.EndDate = new ISODate(doc.EndDate);
        db.ProductListPriceHistory.save(doc);
    });

var ModifiedDate = db.ProductListPriceHistory.find({ ModifiedDate: { $ne: "NULL" } }, {}).toArray();
ModifiedDate.forEach(
    function (doc) {
        doc.ModifiedDate = new ISODate(doc.ModifiedDate);
        db.ProductListPriceHistory.save(doc);
    });


//-- Transformação 4 -- 
//Embutir ProductPriceHistory em ProductDetails 
var newProducts = db.ProductDetails.aggregate([
    {
        $lookup: {
            from: "ProductListPriceHistory",
            localField: "ProductID",
            foreignField: "ProductID",
            as: "PriceHistory",
        },
    },
]).toArray();

//Inserir novos dados numa nova coleção
newProducts.forEach(function (doc) {
    db.Products.insertOne(doc);
});


var emptyPriceHistory = db.Products.find({ PriceHistory: { $exists: true, $size: 0 } }).toArray();

//-- Transformação 5 -- 
//Inserir novos dados numa nova coleção
emptyPriceHistory.forEach(function (doc) {
    db.ProductsList.insertOne(doc);
});

var prodPrice = db.Products.aggregate([
    { $unwind: "$PriceHistory" },
    {
        $group: {
            _id: "$ProductID",
            Name: { $first: "$Name" },
            ProductNumber: { $first: "$ProductNumber" },
            Color: { $first: "$Color" },
            ListPrice: { $first: "$ListPrice" },
            SellStartDate: { $first: "$SellStartDate" },
            SellEndDate: { $first: "$SellEndDate" },
            PriceHistory: { $push: "$PriceHistory" },
        },
    },
]
).toArray();

//Juntar os produtos com e sem histórico de preço
prodPrice.forEach(function (doc) {
    db.ProductsList.insertOne(doc);
});

//Eliminar coleções auxiliares
db.ProductDetails.drop();
db.ProductListPriceHistory.drop();
db.Products.drop();


//-- Transformação 6 -- 
//Embutir o CurrencyDetails no SalesDetails
var newSales = db.SalesDetails.aggregate([
    {
        $lookup: {
            from: "CurrencyDetails",
            localField: "CurrencyRateID",
            foreignField: "CurrencyRateID",
            as: "CurrencyRateID",
        },
    },
]).toArray();

//Inserir novos dados numa nova coleção
newSales.forEach(function (doc) {
    db.salesDetails.insertOne(doc);
});

//Eliminar a antiga coleção
db.SalesDetails.drop();

//Agregar todos os produtos sem currency 
var emptyCurrency = db.salesDetails.find({ CurrencyRateID: { $exists: true, $size: 0 } }).toArray();

//-- Transformação 7 -- 
var salesCurrency = db.salesDetails.aggregate([{ $unwind: "$CurrencyRateID" },]).toArray();

//Inserir dados com currency numa coleção
salesCurrency.forEach(function (doc) {
    db.Sales.insertOne(doc);
});

//Inserir dados sem Currency na colecao anterior
emptyCurrency.forEach(function (doc) {
db.Sales.insertOne(doc);
});

//Apagar coleções auxiliares e a coleção embutida
db.SalesDetails.drop();
db.salesDetails.drop();
db.CurrencyDetails.drop();

//-- Transformação 8 -- 
//Alterar o preço pelo mais recente no histórico de preços diferente de 0
var newProducts = db.ProductsList.aggregate([{
    "$match": {
        $and: [{ ListPrice: { $eq: 0.00 } },
        { "PriceHistory.0.Price": { $ne: 0.00 } },]
    }
},
{ $set: { "ListPrice": { $slice: ["$PriceHistory.Price", -1] } } },
{ $unwind: "$ListPrice" },

]).toArray();

//Guardar o resultado da agregação na base de dados
newProducts.forEach(function (doc) {
    db.Products.insertOne(doc);
});

//-- Transformação 9 --
//Guardar os produtos com ListPrice diferente de 0
var newProducts = db.ProductsList.aggregate([{
    "$match": {
        ListPrice: { $ne: 0.00 }
    }
}]).toArray();

//Inserir novos dados numa nova coleção de Products com e sem CurrencyID
newProducts.forEach(function (doc) {
    db.Products.insertOne(doc);
});
db.ProductsList.drop();

//-- Transformação 10 --
//Colocar os tipos de dados corretos
var newProducts = db.Products.aggregate([
    {
        $addFields: {
            ListPrice: { $toDouble: { $substrBytes: ["$ListPrice", 0, 8] } },
        }
    },
]).pretty();

newProducts.forEach(function (doc) {
    db.ProductsList.insertOne(doc);
});

db.Products.drop();









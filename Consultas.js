//---------Transformação 1----------------
//Obter uma listagem do número total de unidade vendidas por produto para um determinado
//período. Os resultados devem estar ordenados de forma descendente pelo número de unidades
//vendidas

var countProductsPerDate = db.Sales.aggregate([
{
 $match:{ OrderDate:{$gte:ISODate("2012-02-10"), $lte:ISODate("2013-02-10")}}
},
{ $unwind: "$ReceiptLines" },
 {
    $group: {
        "_id":{
             "ID":"$ReceiptLines.Product._id",
            "Name": "$ReceiptLines.Product.Name" ,
        },
        "count":{$sum:"$ReceiptLines.Quantity"}
    }
},
{
 "$sort":{"count":-1}
}]).toArray();

//----------Transformação 2-----------
//Obter uma listagem do número o total de unidade vendidas por cliente para um determinado
//período. Os resultados devem estar ordenados de forma descendente pelo número de unidades
//vendidas

var countProductsPerCustomer = db.Sales.aggregate([
{
 $match:{ OrderDate:{$gte:ISODate("2012-02-10"), $lte:ISODate("2013-02-10")}}
},
{ $unwind: "$ReceiptLines" },
 {
    $group: {
        "_id":{
             "ID":"$Customer._id",
            "FirstName": "$Customer.Name.First",
            "LastName": "$Customer.Name.Last",
        },
        "TotalProducts":{$sum:"$ReceiptLines.Quantity"}
    }
},
{
 "$sort":{"TotalProducts":-1}
}]).toArray();

//----------Transformação 3-----------
//Obter uma listagem das vendas em que não existe um cliente válido associado (presente no
//documento CustomerDetails).

var c = db.Sales.aggregate([
   {
        $lookup: {
            from: "Customer",
            localField: "Customer._id",
            foreignField: "_id",
            as: "Testando",
        },
    },
    {
     $match:{Testando: { $exists: true, $size: 0 }}
    },
     {
     $project:{
      _id:1,
      OrderDate:1,
      SubTotal:1,
      TaxAmt:1,
      Store:1,
      ReceiptLines:1
     }
    },
]).toArray();


//----------Transformação 4-----------
//Obter uma listagem dos produtos descontinuados
db.ProductsList.find({SellEndDate:{$ne:"NULL"}},{}).pretty()


//----------Transformação 5-----------
//Obter uma listagem dos clientes que compram nenhum produto há mais de um mês;
var month= new Date(ISODate().getTime() - 1000 * 86400 * 30);

var res = db.Sales.aggregate(
    [
        {
            $group:
                {
                    _id: "$Customer._id",
                    Customer: { $first: "$Customer" },
                    OrderDate: { $first: "$OrderDate" },
                    maxi: { $max: "$OrderDate" }
                }
        },
         {
        $match: { 
                maxi: { $lt:month }
            }
        },
        {
         $project:
         {
              Customer:1,
              LastOrder:"$maxi"  
         }   
        },
    ]
).pretty();


//----------Transformação 6-----------
//Obter uma listagem dos produtos que não vendidos há mais de uma semana.
var week= new Date(ISODate().getTime() - 1000 * 86400 * 7);

var res = db.Sales.aggregate(
    [
    {
      $unwind:"$ReceiptLines"  
    },
       
 {
            $group:
                {
                    _id: "$ReceiptLines.Product._id",
                    Name: { $first: "$ReceiptLines.Product.Name" },
                    OrderDate: { $first: "$OrderDate"},
                    maxi: { $max: "$OrderDate" }
                }
        },
         {
        $match: { 
                maxi: { $lt:week }
            }
        },
        {
         $project:
         {
              Name:1,
              ProductID:1,
              LastOrder:"$maxi"  
         }   
        },
    ]
).pretty();










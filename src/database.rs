use crate::{Pessoa, TransactionDTS, PessoaDTS};
use reqwest::StatusCode;
use sqlx::{PgPool,postgres::PgPoolOptions, Row};
use time::Time;
use uuid::Uuid;
use time::OffsetDateTime;

pub struct Repository{
    pool: PgPool, 
}

impl Repository {
    pub async fn conn(url : String) -> Self {
        Repository{
        pool : PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap(),
        }
    }

    pub async fn createPessoa(&self  , newperson:PessoaDTS) -> Result<Pessoa, sqlx::Error>{
    let idtemp = Uuid::now_v7();
    sqlx::query_as(
        " 
        INSERT INTO Pessoa (id, name, email, CPF, balance, tipo, password)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, email, CPF, balance, tipo, password
        ",
    )
    .bind(idtemp)
    .bind(newperson.name)
    .bind(newperson.email)
    .bind(newperson.cpf) 
    .bind(newperson.balance)
    .bind(newperson.tipo)
    .bind(newperson.password)
    .fetch_one(&self.pool)
    .await
    
    }

   pub async fn createTransaction(&self  , newTransaction:TransactionDTS) -> Result<Pessoa, sqlx::Error>{    
    
    match self.update_balance_of_payee(newTransaction.clone()).await{
        Ok(_) => {},
        Err(e) => {
            return Err(e)
        }
    }

    match self.update_balance_of_payer(newTransaction.clone()).await{
        Ok(_) => {},
        Err(e) => {
            return Err(e)
        }
        
    }
    
    sqlx::query_as(
        "
        INSERT INTO transaction (id, payee, payer, amount, date)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, payee, payer, amount, date
    ",
    )
    .bind(Uuid::now_v7()) // Gerar um novo UUID para a transação
    .bind(newTransaction.payee)
    .bind(newTransaction.payer)
    .bind(newTransaction.amont)
    .bind(time::OffsetDateTime::now_utc())
    .fetch_one(&self.pool)
    .await   
    
   
    
}

async fn update_balance_of_payee(&self ,data:TransactionDTS) -> Result<(), sqlx::Error>{   
    sqlx::query_as(
        "
        UPDATE Pessoa
        SET balance = balance + $1
        WHERE id = $2
        "
    )
    .bind(data.amont)
    .bind(data.payee)
    .fetch_one(&self.pool)
    .await
}

async fn update_balance_of_payer(&self ,data:TransactionDTS) -> Result<(), sqlx::Error>{   
    sqlx::query_as(
        "
        UPDATE Pessoa
        SET balance = balance - $1
        WHERE id = $2
        "
    )
    .bind(data.amont)
    .bind(data.payee)
    .fetch_one(&self.pool)
    .await
}



    pub async fn searchPessoa(&self  , query: String) ->Result<Option<Pessoa>, sqlx::Error>{
        sqlx::query_as("
            SELECT * 
            FROM Pessoa 
            WHERE to_tsquery('people',$1) @@ search
            LIMIT 50
        ",
        )
        .bind(query)
        .fetch_optional(&self.pool)
        .await
    
    }

    pub async fn findPessoa(&self  , id:Uuid) ->Result<Option<Pessoa>, sqlx::Error>{
        sqlx::query_as("
            SELECT * FROM Pessoa WHERE id=$1
        ",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await 
    
    } 


}

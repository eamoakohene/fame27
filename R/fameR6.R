fameR6 <- R6::R6Class(
  'fameR6',
  public = list(
     code = NULL,
     sic_level = 2,
     sic_excl = '27',

     initialize = function(code,lvl = 2,exclude='27',db_mode=T){

       self$set_code(code)
       self$set_level(lvl)
       self$set_exclusion(exclude)
       private$set_db_mode(db_mode)

     },

     set_code = function(value){
       if(!missing(value) && !is.null(value) ){
          self$code <- value
       }
       invisible(self)
     },

     set_level = function(value){
       if(!missing(value) && !is.null(value) ){
         self$sic_level <- value
       }
       invisible(self)
     },

     set_exclusion = function(value){
       if(!missing(value) && !is.null(value) ){
         self$sic_excl <- value
       }
       invisible(self)
     },

     get_sql = function(short=TRUE){

       my_sql <- NULL
       if(!short){
            my_sql <- paste0("select distinct company, db,rn,sic,description from fame27 where sic like '",self$code,"%';")
       }else{
            my_sql <- paste0("select distinct sic,description from fame27 where sic like '",self$code,"%';")
       }

       return(my_sql)
     },

     get_sics = function(){
        my_sics <- private$run_sql(
                        paste0("select distinct substr(sic,1,",self$sic_level,") as sics from fame27 order by sics;")
                  )
        return(
           dplyr::filter(my_sics, !(trimws(sics)==""), !(sics == self$sic_excl) )
        )
     },

     get_data = function(){

       my_data <- private$run_sql(self$get_sql())
       return(my_data)
     },

     get_top = function(
       db = c('key','pri','sec'),
       field = c('rev','emp')
     ){

       my_db <- match.arg(db)
       my_field <- match.arg(field)

       sql_source <- switch( my_db,
                           'key' = 'keyword_top',
                           'pri' = 'primary_top',
                           'sec' = 'secondary_top'
        )

       sql_field <- switch( my_field,
                      'rev' = 'revenue',
                      'emp' = 'employment'

       )

       my_sql <- sprintf("select company, %s from fame27 where db= '%s' ", sql_field,sql_source)


       my_data <- private$run_sql(my_sql)

       if(sql_field == 'revenue'){

         my_data$revenue <- as.numeric( my_data$revenue )
         my_data <- dplyr::filter( my_data, !is.na(  revenue ) )
         my_data <- dplyr::arrange( my_data, desc(revenue) )

       }else{

         my_data$employment <- as.numeric( my_data$employment )
         my_data <- dplyr::filter( my_data, !is.na(  employment ) )
         my_data <- dplyr::arrange( my_data, desc(employment) )

       }

       return(my_data)

     },

     get_data_all = function(){

       my_sics <- self$get_sics()
       my_rows <- nrow(my_sics)
       if( my_rows==0){ cat('No sic data. Exiting ....\n')}

       self$set_code(my_sics$sics[1])
       my_data <- self$get_data()

       if(my_rows >1 ){
         for(i in 2:my_rows){
           self$set_code(my_sics$sics[i])
           my_data_i <- self$get_data()
           my_data <- rbind(my_data,my_data_i)
         }

       }
       private$clip(my_data)
       return(my_data)
     }

     ,get_exclusion = function(is_company=FALSE){

       my_exclusion <- NULL

       if(!is_company){

           my_exclusion <- private$run_sql("select * from exclusion order by sic;")

       }else{

         my_exclusion <-private$run_sql(
               "select distinct company,sic,db from fame27 where sic in (select distinct sic from exclusion) order by company,db;"
             )

       }

       private$clip(my_exclusion)
       return( my_exclusion )
     }
  ),

  private = list(

    db_local= 'R:/packages/fame27/inst/extdata/fame.sqlite',
    db_package = system.file("extdata/fame.sqlite",package="fame27"),
    db_path = NULL,


    set_db_mode = function(is_local=TRUE){
      if(is_local){

        private$db_path <- private$db_local

      }else{

        private$db_path <- private$db_package

      }
    },

    run_sql = function(q){
      sqldf::sqldf(q,dbname= private$db_path)
    },

    clip = function(x,row.names=FALSE,col.names=TRUE,...) {
      write.table(
         x,
         "clipboard",
         sep="\t",
         row.names=row.names,
         col.names=col.names,
         ...
      )
    }
  )
)

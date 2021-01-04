reset_env <- function(init_env){
  final_env <- ls(.GlobalEnv)

  rm(
    envir = .GlobalEnv,
    list = setdiff(final_env, init_env)
  )

  gc()
}

build_repo <- function(module){
  repo <- here("data", "clean", module)

  if(!dir.exists(repo)){
    dir.create(repo)
  }else{
    unlink(repo, recursive = T)
    dir.create(repo)
    }
  }

run_module <- function(module, domain) {
  print(
    paste0("running module ", module, "...")
  )

  build_repo(module)

  source_file <- paste0("run_", module, ".R")
  path <- here("source", "modules", domain, source_file)

  init_env <- ls(.GlobalEnv)
  source(path)
  reset_env(init_env)

  print(
    paste("module", module, "complete!")
  )
}

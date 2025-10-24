# =============================================================================
# REPLICATION STUDY — Determinants of Household Electricity Prices in the EU
# Da Silva & Cerqueira (2017)
# System-GMM Panel Data Replication — Enhanced & Verified Version
# =============================================================================

# --- Installation et chargement des packages nécessaires ---------------------
required_packages <- c("eurostat", "dplyr", "plm", "tidyr", "readr", "countrycode",
                       "mice", "stargazer", "ggplot2", "sandwich", "lmtest", "purrr")
library(readr)
for(pkg in required_packages) {
  if(!require(pkg, character.only = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

options(stringsAsFactors = FALSE)
set.seed(12345) # Loïc, Loïc + Quentin, Loïc + Quentin + Yohan, 
#Loïc + Quentin + Yohan + Maxime, Loïc + Quentin + Yohan + Maxime + Aurelien  

# =============================================================================
# 1. FONCTIONS DE TÉLÉCHARGEMENT ET DE PRÉPARATION
# =============================================================================

# Gestion robuste du téléchargement depuis Eurostat
safe_get_raw <- function(code, filters = NULL){
  tryCatch({
    message("-> get_eurostat: ", code)
    df <- get_eurostat(code, filters = filters, time_format = "date")
    if(is.null(df) || nrow(df) == 0){
      message("   (vide) ", code)
      return(NULL)
    }
    message("   OK (", nrow(df), " lignes) : ", code)
    return(df)
  }, error = function(e){
    message("   Erreur get_eurostat pour ", code, " : ", e$message)
    return(NULL)
  })
}

# Essayons une liste d'identifiants et renvoie le premier non-null (avec filtres si fournis)
try_codes <- function(codes, filters = NULL){
  for(code in codes){
    df <- safe_get_raw(code, filters = filters)
    if(!is.null(df)) return(list(code = code, df = df))
  }
  return(NULL)
}

# Préparation uniforme des données Eurostat
prepare_eurostat_data <- function(df, valname) {
  if (is.null(df)) return(NULL)
  
  # Détection du nom correct de la colonne temporelle
  time_col <- intersect(names(df), c("time", "TIME_PERIOD", "date"))[1]
  if (is.na(time_col)) stop("Colonne temporelle non trouvée dans le jeu de données.")
  
  # Détection du nom de la colonne de valeur
  value_col <- intersect(names(df), c("values", "value"))[1]
  if (is.na(value_col)) stop("Colonne de valeurs non trouvée.")
  
  # Transformation standardisée
  df %>%
    rename(value = all_of(value_col),
           time = all_of(time_col)) %>%
    select(geo, time, value) %>%
    mutate(
      geo = as.character(geo),
      time = as.integer(format(as.Date(as.character(time)), "%Y"))
    ) %>%
    rename(!!valname := value)
}


# =============================================================================
# 2. TÉLÉCHARGEMENT AUTOMATIQUE DES SÉRIES EUROSTAT
# =============================================================================

# ---------- candidates lists. @Quentin, tu pourras l'étendre si nécessaire ----------
candidates <- list(
  Ep = c("nrg_pc_204_h", "nrg_pc_204", "nrg_pc_204_c", "nrg_pc_204_a"),
  GASp = c("nrg_pc_202_h", "nrg_pc_202", "nrg_pc_202_c"),
  RESe = c("nrg_ind_ren","nrg_ind_ren_a"),
  GDPpc = c("nama_10_pc","nama_10_pc_a"),
  # pour la conso élec (souvent lourde) on va chercher plusieurs identifiants possibles
  ECH = c("nrg_bal_c", "nrg_bal", "nrg_101a", "nrg_ind_consumption"),
  # GES: candidates basiques
  GGE = c("env_air_gge", "env_air_gge_a", "env_air_pollutant")
)

# filtres par dataset (liste nom->filters)
filters_map <- list(
  Ep = list(tax = "TOTAL", currency = "EUR", unit = "KWH"),
  GASp = list(tax = "TOTAL", currency = "EUR", unit = "KWH"),
  GDPpc = list(unit = "CP_EUR_HAB"),
  # pour ECH et GGE on commence sans filtres (puis on filtrera localement)
  ECH = NULL,
  GGE = NULL,
  RESe = NULL
)

eu_countries <- c("BE","CZ","DK","DE","EE","IE","GR","ES","FR","IT",
                  "CY","LV","LT","HU","MT","PL","PT","RO","SI","SK",
                  "FI","SE","UK")
year_min <- 2000; year_max <- 2014

message("Début du téléchargement des données Eurostat...")

results <- list()
for(var in names(candidates)){
  message("\n=== Recherche pour : ", var, " ===")
  filt <- filters_map[[var]]
  res <- try_codes(candidates[[var]], filters = filt)
  if(is.null(res)){
    message("Aucun identifiant trouvé automatiquement pour ", var, ". On tente une recherche textuelle (search_eurostat)...")
    # lister les 10 premières correspondances
    hits <- search_eurostat(var)
    if(length(hits)>0){
      print(head(hits, 10))
    } else {
      message("Aucune correspondance trouvée par search_eurostat pour: ", var)
    }
    # signale à l'utilisateur qu'il doit fournir un CSV local
    message("=> ACTION REQUISE : si l'API ne renvoie rien, fournissez un fichier CSV local nommé '", var, ".csv' dans le répertoire de travail.")
    # tenter lecture CSV si existant
    if(file.exists(paste0(var, ".csv"))){
      message("Lecture du fichier local ", var, ".csv")
      results[[var]] <- read_csv(paste0(var, ".csv"))
    } else {
      results[[var]] <- NULL
    }
  } else {
    # préparer le tableau
    df_prep <- prepare_eurostat_data(res$df, var)
    if(!is.null(df_prep)){
      # réduire vite à l'intervalle et aux pays avant de mettre en mémoire
      df_prep <- df_prep %>% filter(geo %in% eu_countries, time >= year_min, time <= year_max)
      results[[var]] <- df_prep
      rm(res); gc()
    } else {
      message("Préparation échouée pour ", res$code, " — stockage en NULL")
      results[[var]] <- NULL
    }
  }
}

# =============================================================================
# 3. FUSION, NETTOYAGE DES DONNÉES ET AFFICHAGE DES RESULTATS
# =============================================================================

sapply(results, function(x) if(is.null(x)) NA_integer_ else nrow(x))

mandatory <- c("Ep","GASp","RESe","GDPpc")
missing_mandatory <- setdiff(mandatory, names(Filter(Negate(is.null), results)))
if(length(missing_mandatory) < length(mandatory)){
  message("⚠️ Certains jeux obligatoires manquent : ", paste(setdiff(mandatory, names(Filter(Negate(is.null), results))), collapse = ", "))
  message("Si un jeu manque, vous devez fournir un CSV local nommé Var.csv (par ex. Ep.csv) ou obtenir le dataset via le site Eurostat/Comext.")
}

to_merge <- Filter(Negate(is.null), results)
if(length(to_merge) == 0) stop("Aucun jeu disponible pour fusionner. Fournir des CSV locaux.")

merged <- NULL
for(name in names(to_merge)){
  if(is.null(merged)){
    merged <- to_merge[[name]]
  } else {
    merged <- full_join(merged, to_merge[[name]], by = c("geo","time"))
  }
  gc()
  message("Merged progressive: ", name, " -> rows:", nrow(merged))
}

###final_data <- merged %>% arrange(geo, time)

final_data <- merged %>% arrange(geo, time)

# If ECH or GGE are present but huge, allow manual extra-filtering:
# Now transformations and GMM setup (example minimal)
final_data <- final_data %>%
  left_join(data.frame(geo = eu_countries, Lib_year = rep(2007, length(eu_countries))), by = "geo") %>%
  mutate(Lib = ifelse(time >= Lib_year, 1, 0),
         RESe = ifelse(!is.na(RESe), RESe/100, NA),
         Lep = ifelse(!is.na(Ep) & Ep>0, log(Ep), NA),
         L_GDPpc = ifelse(!is.na(GDPpc) & GDPpc>0, log(GDPpc), NA))

final_small <- final_data %>% select(geo, time, Lep, L_GDPpc, RESe, Lib)

imp <- mice(final_small %>% select(Lep, L_GDPpc, RESe), m=3, maxit=5, print=FALSE)
final_imp <- complete(imp,1)
final_small <- bind_cols(final_small %>% select(geo,time,Lib), final_imp)

pdata <- pdata.frame(final_small, index = c("geo","time"))
model <- pgmm(Lep ~ lag(Lep,1) + L_GDPpc + RESe + Lib |
                lag(Lep,2:4) + lag(L_GDPpc,2:4) + lag(RESe,2:4),
              data=pdata, effect="individual", model="twostep", transformation="ld", collapse=TRUE)
summary(model)

# save
saveRDS(list(results = results, merged = merged, model = model), "robust_download_results.rds")
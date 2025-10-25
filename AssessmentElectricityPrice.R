# =============================================================================
# REPLICATION STUDY — Determinants of Household Electricity Prices in the EU
# Da Silva & Cerqueira (2017)
# System-GMM Panel Data Replication — Enhanced & Verified Version
# =============================================================================

# =============================================================================
# 0. PRÉAMBULE — Chargement des packages
# =============================================================================
required_packages <- c("eurostat", "dplyr", "plm", "tidyr", "readr", "countrycode",
                       "mice", "stargazer", "ggplot2", "sandwich", "lmtest", "purrr")
for(pkg in required_packages) {
  if(!require(pkg, character.only = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}
library(dplyr)
library(lubridate)
options(stringsAsFactors = FALSE)
set.seed(12345)

# =============================================================================
# 1. PARAMÈTRES GÉNÉRAUX
# =============================================================================
eu_countries <- c("BE","CZ","DK","DE","EE","IE","GR","ES","FR","IT",
                  "CY","LV","LT","HU","MT","PL","PT","RO","SI","SK",
                  "FI","SE","UK")
year_min <- 2000; year_max <- 2015

# =============================================================================
# 2. IMPORTATION / TÉLÉCHARGEMENT DES DONNÉES
# =============================================================================

safe_get_raw <- function(code, filters=NULL){
  tryCatch({
    df <- get_eurostat(code, filters=filters, time_format="date")
    if(is.null(df) || nrow(df)==0) return(NULL)
    df
  }, error=function(e) NULL)
}

prepare_eurostat_data <- function(df, valname){
  if(is.null(df)) return(NULL)
  time_col <- intersect(names(df), c("time", "TIME_PERIOD", "date"))[1]
  value_col <- intersect(names(df), c("values", "value", "OBS_VALUE"))[1]
  df %>%
    rename(value = !!value_col, time = !!time_col) %>%
    select(geo, time, value) %>%
    mutate(time = as.integer(format(as.Date(as.character(time)), "%Y"))) %>%
    rename(!!valname := value)
}

# --- Lecture intelligente (local CSV ou API) ---
load_or_download <- function(name, code, filters=NULL){
  file <- paste0(name, ".csv")
  if(file.exists(file)){
    message("Lecture locale : ", file)
    df <- read_csv(file, show_col_types = FALSE)
    df <- df %>%
      mutate(geo = ifelse("geo" %in% names(df), geo, STRUCTURE_ID),
             time = ifelse("time" %in% names(df), time, as.integer(substr(`Time`,1,4))),
             value = ifelse("values" %in% names(df), values, OBS_VALUE)) %>%
      select(geo, time, value)
    return(df)
  } else {
    message("Téléchargement depuis Eurostat : ", code)
    df <- safe_get_raw(code, filters)
    return(df)
  }
}

Ep_raw   <- load_or_download("Ep",   "nrg_pc_204_h")
GASp_raw <- load_or_download("GASp", "nrg_pc_202_h")
RESe_raw <- safe_get_raw("nrg_ind_ren")
GDPpc_raw <- safe_get_raw("nama_10_pc")
ECH_raw  <- safe_get_raw("nrg_bal_c")
GGE_raw  <- load_or_download("GGE",  "env_air_gge_h") #J'ai volontairement modifié 
#l'indice du dataset des GGE car il est complètement lourd lorsque téléchargé. 

# =============================================================================
# 3. PRÉPARATION ET AGRÉGATION
# =============================================================================

aggregate_large <- function(df, varname){
  df %>%
    filter(geo %in% eu_countries) %>%
    mutate(year = as.integer(format(as.Date(as.character(time)), "%Y"))) %>%
    group_by(geo, year) %>%
    summarise(value = mean(values, na.rm=TRUE)) %>%
    rename(time = year, !!varname := value)
}

ep   <- prepare_eurostat_data(Ep_raw, "Ep")
gasp <- prepare_eurostat_data(GASp_raw, "GASp")
rese <- prepare_eurostat_data(RESe_raw, "RESe")
gdp  <- prepare_eurostat_data(GDPpc_raw, "GDPpc")
gge  <- prepare_eurostat_data(GGE_raw, "GGEpc")

# --- agrégation spéciale pour ECH ---
if(!is.null(ECH_raw)){
  ECH_small <- ECH_raw %>%
    filter(nrg_bal == "FC_OTH_HH_E", siec == "E7000", unit == "KTOE") %>%
    mutate(TIME_PERIOD = year(TIME_PERIOD)) %>%    # ⬅️ Extraire uniquement l'année
    group_by(geo, TIME_PERIOD) %>%
    summarise(ECHpc = mean(values, na.rm=TRUE), .groups="drop")%>%
    rename(time = TIME_PERIOD)
} else {
  ECH_small <- NULL
}
#print(distinct(ECH_raw, nrg_bal, siec, unit), n=500 )
#View(ECH_raw)
# Fusion allégée
dfs <- list(ep, gasp, rese, gdp, ECH_small, gge)
lapply(dfs, names) # pour vérifier si d'autres datasets n'ont pas "name" commme 
#nonm de colonne.

panel_data <- dfs %>%
  purrr::compact() %>%
  purrr::reduce(full_join, by=c("geo","time")) %>%
  filter(geo %in% eu_countries, time >= year_min, time <= year_max)

# =============================================================================
# 4. VARIABLES DE POLITIQUE
# =============================================================================

policy_data <- data.frame(
  geo = eu_countries,
  Lib_year = c(2007, 2006, 2003, 1998, 2009, 2005, 2007, 2003, 2007, 2007,
               2010, 2007, 2007, 2007, 2010, 2007, 2007, 2007, 2007, 2007,
               1997, 1996, 1990),
  Reg10 = c(0,0,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,0,0,0)
)

final_data <- panel_data %>%
  left_join(policy_data, by = "geo") %>%
  mutate(
    Lib = ifelse(time >= Lib_year, 1, 0),
    RESe = RESe / 100
  )

# =============================================================================
# 5. TRANSFORMATIONS ET IMPUTATION MULTIPLE
# =============================================================================
final_data <- final_data %>%
  mutate(
    Lep = ifelse(Ep > 0, log(Ep), NA),
    L_GDPpc = ifelse(GDPpc > 0, log(GDPpc), NA),
    L_ECHpc = ifelse(ECHpc > 0, log(ECHpc), NA),
    L_GASp = ifelse(GASp > 0, log(GASp), NA),
    L_GGEpc = ifelse(GGEpc > 0, log(GGEpc + 1), NA)
  )

perform_imputation <- function(data){
  imp_vars <- data %>% select(geo, time, Lep, L_GDPpc, L_ECHpc, L_GASp, L_GGEpc, RESe, Lib, Reg10)
  ini <- mice(imp_vars, maxit=0, print=FALSE)
  meth <- ini$meth; pred <- ini$pred
  meth[c("geo","time")] <- ""; pred[, c("geo","time")] <- 0
  imp <- mice(imp_vars, m=5, method="pmm", seed=12345, print=FALSE)
  complete(imp, 1)
}

final_data_imp <- perform_imputation(final_data)

# =============================================================================
# 6. STATISTIQUES DESCRIPTIVES ET VISUALISATION
# =============================================================================
cat("\n=== STATISTIQUES DESCRIPTIVES ===\n")
stargazer(final_data_imp %>%
            select(Lep, L_GDPpc, L_ECHpc, L_GASp, RESe, L_GGEpc, Lib, Reg10),
          type="text", title="Statistiques descriptives", digits=3)

ggplot(final_data_imp, aes(x=time, y=exp(Lep), group=geo)) +
  geom_line(alpha=0.6) +
  geom_smooth(aes(group=1), method="loess", color="red", se=FALSE) +
  theme_minimal()

# =============================================================================
# 7. ESTIMATION GMM
# =============================================================================
pdata <- pdata.frame(final_data_imp, index=c("geo","time"))

estimate_gmm_models <- function(data){
  list(
    pgmm(Lep ~ lag(Lep,1) + L_GDPpc + L_GASp + RESe + Lib |
           lag(Lep,2:4) + lag(L_GDPpc,2:4) + lag(RESe,2:4),
         data=data, effect="individual", model="twostep",
         transformation="ld", collapse=TRUE),
    pgmm(Lep ~ lag(Lep,1) + L_ECHpc + L_GASp + RESe + Lib |
           lag(Lep,2:4) + lag(L_ECHpc,2:4) + lag(RESe,2:4),
         data=data, effect="individual", model="twostep",
         transformation="ld", collapse=TRUE)
  )
}
gmm_models <- estimate_gmm_models(pdata)

# =============================================================================
# 8. DIAGNOSTICS
# =============================================================================
perform_diagnostics <- function(models){
  lapply(models, function(m){
    sm <- summary(m)
    list(summary=sm, ar1=mtest(m, order=1), ar2=mtest(m, order=2))
  })
}
diagnostics <- perform_diagnostics(gmm_models)

# =============================================================================
# 9. COMPARAISON AVEC L’ARTICLE
# =============================================================================
article_coefs <- data.frame(
  Variable=c("lag(Lep, 1)","L_GDPpc","L_GASp","RESe","Lib"),
  Article_Coef=c(0.767,0.158,0.678,1.730,-0.343)
)
our_coefs <- summary(gmm_models[[1]])$coefficients %>%
  as.data.frame() %>%
  tibble::rownames_to_column("Variable") %>%
  select(Variable, Our_Coef=Estimate)
comparison <- left_join(article_coefs, our_coefs, by="Variable") %>%
  mutate(Diff=abs(Our_Coef-Article_Coef), Rel_Diff=Diff/abs(Article_Coef))

# =============================================================================
# 10. RAPPORT FINAL
# =============================================================================
generate_report <- function(models, diagnostics, comparison){
  cat("\n", strrep("=",70), "\n")
  cat("RAPPORT FINAL — RÉPLICATION GMM ÉLECTRICITÉ UE\n")
  cat(strrep("=",70), "\n")
  for(i in seq_along(models)){
    sm <- diagnostics[[i]]$summary
    cat("Modèle",i,"| AR(1) p=",round(diagnostics[[i]]$ar1$p.value,4),
        "| AR(2) p=",round(diagnostics[[i]]$ar2$p.value,4),
        "| Hansen p=",round(sm$diagnostics["Hansen",2],4),
        "| Instruments=",sm$diagnostics["Instruments",1],"\n")
  }
  cat("Taux de correspondance (<50% diff):",
      round(mean(comparison$Rel_Diff < 0.5, na.rm=TRUE)*100,1),"%\n")
}
generate_report(gmm_models, diagnostics, comparison)

# =============================================================================
# 11. EXPORTATION
# =============================================================================
saveRDS(list(data=final_data_imp, models=gmm_models,
             diagnostics=diagnostics, comparison=comparison),
        "Replication_GMM_Results_EU_Electricity.rds")
write_csv(comparison, "Comparison_Article_vs_Replication.csv")
cat("\n=== ANALYSE TERMINÉE ===\n")

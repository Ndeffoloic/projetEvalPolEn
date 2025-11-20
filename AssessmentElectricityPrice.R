########################################################################
# SCRIPT FINAL (data.table) — Réplication Da Silva & Cerqueira (2017)
# Variables: Ep, GGEpc, GASp, GDPpc (A: sdg_08_10), RESe, Lib, Reg10
# Output: panel_elec.parquet, panel_elec.rds, descriptives_panel.csv
########################################################################

# ---- 0) Packages ----
pkgs <- c("data.table","eurostat","readxl","arrow")
for(p in pkgs) if(!requireNamespace(p, quietly = TRUE)) install.packages(p)
library(data.table); library(eurostat); library(readxl); library(arrow)

# ---- 1) Paramètres ----
year_min <- 2000L
year_max <- 2014L

eu_codes <- c(
  "BE","CZ","DK","DE","EE","IE","EL","ES","FR","IT","CY","LV","LT",
  "HU","MT","PL","PT","RO","SI","SK","FI","SE","UK"
)

message("Pays : ", paste(eu_codes, collapse = ", "))
message("Période : ", year_min, "-", year_max)

# ---- 2) utilitaires ----
fix_time_column <- function(dt){
  tcand <- intersect(c("time","TIME_PERIOD","year","TIME","Time","Year"), names(dt))
  if(length(tcand) == 0) stop("Aucune colonne temporelle détectée.")
  setnames(dt, tcand[1], "time")
  if(inherits(dt$time, "Date")) dt[, time := as.integer(format(time, "%Y"))]
  else if(is.numeric(dt$time)) dt[, time := as.integer(time)]
  else dt[, time := as.integer(gsub("(-.*)$", "", as.character(time)))]
  invisible(dt)
}

find_value_col <- function(dt){
  vc <- intersect(c("values","value","OBS_VALUE","obsValue"), names(dt))
  if(length(vc) == 0) stop("Aucune colonne de valeurs trouvée.")
  vc[1]
}

safe_get_eurostat_filtered <- function(code, geo_filter, time_min, time_max, keep_cols = NULL){
  message("Téléchargement: ", code)
  raw <- get_eurostat(code, time_format = "num") %>% as.data.table()
  fix_time_column(raw)
  vcol <- find_value_col(raw)
  setnames(raw, vcol, "value")
  # optional: keep only columns needed
  if(is.null(keep_cols)){
    keep_cols <- intersect(c("geo","time","value", "unit","src_crf","freq","airpol"), names(raw))
  }
  raw <- raw[, ..keep_cols]
  # filter time & geo (we keep other dims until we choose filter criteria outside)
  raw <- raw[time >= time_min & time <= time_max & geo %in% geo_filter]
  return(raw)
}

# ---- 3) GASp (BP) ----
if(!file.exists("gas_ts.csv")){
  stop("Fichier gas_ts.csv absent dans le répertoire de travail. Crée-le (Year,GASp) puis relance.")
}
gas_ts <- fread("gas_ts.csv")
# Filtrer période et calculer log
gas_ts <- gas_ts[Year >= year_min & Year <= year_max]
gas_ts[, L_GASp := log(GASp)]
setnames(gas_ts, "Year", "time")  # faciliter join avec time

# ---- 4) GGEpc : sdg_13_10 (Greenhouse gas emissions per capita) ----
# On lit et on filtre par unité et source observées dans ton export : unit=="T_HAB", src_crf=="TOTX4_MEMO"
GGE_id <- "sdg_13_10"
GGE_raw <- get_eurostat(GGE_id, time_format="num") %>% as.data.table()
fix_time_column(GGE_raw)
# détecter colonne value (sdg tables use 'values')
vcol <- find_value_col(GGE_raw)
if(vcol != "values") setnames(GGE_raw, vcol, "values")
# Inspect: possible units are "I90","T_HAB" in ton export — keep T_HAB
if(!("unit" %in% names(GGE_raw))) stop("sdg_13_10 ne contient pas de colonne 'unit' attendue.")
if(!("src_crf" %in% names(GGE_raw))) stop("sdg_13_10 ne contient pas de colonne 'src_crf' attendue.")
# Filtrage dimensionnel final:
GGE_sub <- GGE_raw[
  unit == "T_HAB" & src_crf == "TOTX4_MEMO" & geo %in% eu_codes & time >= year_min & time <= year_max,
  .(geo, time, GGEpc = as.numeric(values))
]
message("GGE_sub lignes: ", nrow(GGE_sub), " (attendu ≈ 345 ; si <345 -> données manquantes Eurostat)")

# ---- 5) Ep : prix électricité (nrg_pc_204) ----
ep_id <- "nrg_pc_204"
ep_raw <- get_eurostat(ep_id, time_format="num") %>% as.data.table()
fix_time_column(ep_raw)
vcol_ep <- find_value_col(ep_raw)
setnames(ep_raw, vcol_ep, "price")

# Filtrons pour ne garder que les données annuelles (moyenne des semestres)
ep_raw <- ep_raw[
  geo %in% eu_codes &
    time >= year_min &
    time <= year_max &
    unit == "KWH" &
    tax == "I_TAX" &  # Prix incluant les taxes
    product == "6000"  # Code produit pour l'électricité (à vérifier)
]

unique(ep_raw$band)
unique(ep_raw$product)
unique(ep_raw$currency)


# Agrégat : moyenne des prix par (geo, time=année)
ep_sub <- ep_raw[, .(Ep = mean(price, na.rm = TRUE)), by = .(geo, time)]

# Calculer le log
ep_sub[, l_Ep := ifelse(is.na(Ep), NA_real_, log(Ep))]

# Vérifions les doublons résiduels
if (nrow(ep_sub[, .N, by = .(geo, time)][N > 1]) > 0) {
  message("ATTENTION : doublons dans Ep après agrégation. Vérifie les dimensions.")
  print(ep_sub[, .N, by = .(geo, time)][N > 1])
} else {
  message("Ep filtré et agrégé : ", nrow(ep_sub), " lignes, sans doublons.")
}


# ---- 6) GDPpc : sdg_08_10 (PIB réel per capita - option A) ----
# sdg_08_10 usually contains GDP per capita in PPS or similar; detect unit and choose reasonable one
gdp_id <- "sdg_08_10"
gdp_raw <- get_eurostat(gdp_id, time_format="num") %>% as.data.table()
fix_time_column(gdp_raw)
vcol_gdp <- find_value_col(gdp_raw)
setnames(gdp_raw, vcol_gdp, "value_gdp")
# Keep plausible GDP unit: check unique(gdp_raw$unit) interactively if needed
# We'll accept all units and convert them later if necessary; here we keep the numeric value
gdp_sub <- gdp_raw[
  unit == "CLV20_EUR_HAB" &
    na_item == "B1GQ" &
    geo %in% eu_codes &
    time >= year_min & time <= year_max,
  .(geo, time, GDPpc = as.numeric(value_gdp))
]

message("GDP_sub lignes: ", nrow(gdp_sub))

# ---- 7) RESe : part des renouvelables (nrg_ind_ren) ----
possible_res_ids <- c("nrg_ind_ren", "nrg_ind_renf", "sdg_07_30", "sdg_07_40")
res_raw <- NULL
for(id in possible_res_ids){
  try({
    tmp <- get_eurostat(id, time_format="num") %>% as.data.table()
    if(nrow(tmp)>0) { res_raw <- tmp; res_id_used <- id; break }
  }, silent = TRUE)
}

if(is.null(res_raw)){
  message("Aucun dataset RESe trouvé automatiquement parmi candidates; RESe sera NA (tu peux fournir ID exact).")
  res_sub <- data.table(geo = eu_codes, time = seq(year_min, year_max), RESe = NA_real_)[, .(geo, time, RESe)]
} else {
  fix_time_column(res_raw)
  vcol_res <- find_value_col(res_raw)
  setnames(res_raw, vcol_res, "value_res")
  
  # Filtrer pour obtenir la part des renouvelables dans l'électricité (REN_ELC)
  res_sub <- res_raw[
    geo %in% eu_codes &
      time >= year_min &
      time <= year_max &
      nrg_bal == "REN_ELC" &  # Sélectionner uniquement la part dans l'électricité
      unit == "PC",           # Unité en pourcentage
    .(geo, time, RESe = as.numeric(value_res))
  ]
  
  # Vérifier les doublons résiduels (au cas où)
  if (nrow(res_sub[, .N, by=.(geo,time)][N > 1]) > 0) {
    message("ATTENTION : doublons résiduels dans RESe après filtrage. Vérifie les dimensions.")
    print(res_sub[, .N, by=.(geo,time)][N > 1])
  } else {
    message("RESe chargé depuis: ", res_id_used, " (", nrow(res_sub), " lignes, sans doublons)")
  }
}


# ---- 8) Lib / Reg10 (CSV fourni) ----
# lib_reg10.csv must contain: geo, lib_year, reg10 (0/1)
if(file.exists("lib_reg10.csv")){
  libtab <- fread("lib_reg10.csv")
  if(!all(c("geo","lib_year","reg10") %in% names(libtab))){
    stop("lib_reg10.csv doit contenir au moins les colonnes: geo, lib_year, reg10")
  }
} else {
  message("lib_reg10.csv absent : Lib et Reg10 seront créés comme NA (tu peux fournir le CSV).")
  libtab <- NULL
}

# ---- 9) Construire panel final (data.table joins) ----
# Start from ep_sub (one row per country-year expected)
setkey(ep_sub, geo, time)
setkey(GGE_sub, geo, time)
setkey(gdp_sub, geo, time)
setkey(res_sub, geo, time)
setkey(gas_ts, time)  # gas_ts has column 'time' already

# Merge stepwise (left join on ep_sub)
panel <- copy(ep_sub)
panel <- merge(panel, GGE_sub, by = c("geo","time"), all.x = TRUE)
panel <- merge(panel, gdp_sub, by = c("geo","time"), all.x = TRUE)
panel <- merge(panel, res_sub, by = c("geo","time"), all.x = TRUE)
# join gas_ts by time only
panel <- merge(panel, gas_ts[, .(time, GASp, L_GASp)], by = "time", all.x = TRUE)

# Add Lib / Reg10 if provided
if(!is.null(libtab)){
  # Set key for libtab
  setkey(libtab, geo)
  # Create columns Lib and Reg10 in panel
  panel[, Lib := NA_integer_]
  panel[, Reg10 := NA_integer_]
  # Fill Reg10 by joining with libtab
  panel[libtab, Reg10 := i.reg10, on = "geo"]
  # Fill Lib: for each geo, set Lib=1 if time >= lib_year, else 0
  panel[, Lib := as.integer(time >= libtab$lib_year[match(geo, libtab$geo)]), by = geo]
}


# reorder columns
setcolorder(panel, c("geo","time","Ep","l_Ep","GGEpc","GASp","L_GASp","GDPpc","RESe","Lib","Reg10"))

# ---- 10) Vérifications & diagnostics ----
message("Panel lignes: ", nrow(panel))
message("Pays distincts dans panel: ", length(unique(panel$geo)))
message("Années min/max: ", min(panel$time, na.rm=TRUE), "/", max(panel$time, na.rm=TRUE))

# Count missing by variable
missing_counts <- sapply(panel[, .(Ep,GGEpc,GASp,GDPpc,RESe,Lib,Reg10)], function(x) sum(is.na(x)))
print(data.table(variable = names(missing_counts), missing = as.integer(missing_counts)))

# If expected dimensions too large (sanity)
expected_max <- length(eu_codes) * (year_max - year_min + 1)
if(nrow(panel) > expected_max * 5) warning("Panel très grand (plusieurs fois le nombre attendu) — vérifier doublons dans sources.")

# Check duplicates
dups <- panel[, .N, by = .(geo,time)][N>1]
if(nrow(dups) > 0) {
  message("ATTENTION: doublons (geo,time) détectés : ", nrow(dups), " cas. Inspecte sources.")
  print(head(dups))
}

# ---- 11) Statistiques descriptives pour comparaison article ----
desc_vars <- c("Ep","GGEpc","GASp","GDPpc","RESe")
desc_tab <- rbindlist(lapply(desc_vars, function(v){
  if(!(v %in% names(panel))) return(NULL)
  vec <- panel[[v]]
  data.table(variable = v,
             mean = mean(vec, na.rm=TRUE),
             sd   = sd(vec, na.rm=TRUE),
             median = median(vec, na.rm=TRUE),
             n_obs = sum(!is.na(vec)))
}), use.names = TRUE, fill = TRUE)

fwrite(desc_tab, "descriptives_panel.csv")
message("Descriptives exportées -> descriptives_panel.csv")
print(desc_tab)

# ---- 12) Sauvegardes ----
write_parquet(panel, "panel_elec.parquet")
saveRDS(panel, file = "panel_elec.rds")
message("Exports : panel_elec.parquet , panel_elec.rds")

# ---- 13) Petits extras: affichage d'un échantillon ----
print(head(panel, 20))

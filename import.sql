DROP TABLE IF EXISTS `air_temperature`;

CREATE TABLE `air_temperature` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_9` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `tt_tu` float NOT NULL COMMENT '2m air temperature in C°',
    `rf_tu` float NOT NULL COMMENT '2m relative humidity in %',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `cloudiness` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_8` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `v_n_i` char NOT NULL COMMENT 'Index how measurement is taken (P = human Person; I = Instrument)',
    `v_n` tinyint NOT NULL COMMENT 'Total cloud cover (-1 = not determined; 1-8 / 8)',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `precipitation` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_8` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `r1` float NOT NULL COMMENT 'Hourly precipitation height in mm',
    `rs_ind` smallint NOT NULL COMMENT '0 = No precipitation; 1 = Precipitation has fallen',
    `wrtr` smallint NOT NULL COMMENT 'Form of precipitation (WR-code)',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `pressure` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_8` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `p` float NOT NULL COMMENT 'Mean sea level pressure in hPA',
    `p0` float NOT NULL COMMENT 'Pressure at station height in hPA',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `soil_temperature` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_2` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `v_te002` float NOT NULL COMMENT 'Soil temperature in 2 cm depth in C°',
    `v_te005` float NOT NULL COMMENT 'Soil temperature in 5 cm depth in C°',
    `v_te010` float NOT NULL COMMENT 'Soil temperature in 10 cm depth in C°',
    `v_te020` float NOT NULL COMMENT 'Soil temperature in 20 cm depth in C°',
    `v_te050` float NOT NULL COMMENT 'Soil temperature in 50 cm depth in C°',
    `v_te100` float NOT NULL COMMENT 'Soil temperature in 100 cm depth in C°',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `solar` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_started_at` datetime NOT NULL,
    `measured_ended_at` datetime NOT NULL,
    `qn_592` tinyint NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `atmo_lberg` float NOT NULL COMMENT 'Hourly sum of longwave downward radiation in J/cm^2',
    `fd_lberg` float NOT NULL COMMENT 'Hourly sum of diffuse solar radiation in J/cm^2',
    `fg_lberg` float NOT NULL COMMENT 'Hourly sum of solar incoming radiation in J/cm^2',
    `sd_lberg` float NOT NULL COMMENT 'Hourly sum of sunshine duration in minutes',
    `zenit` float NOT NULL COMMENT 'Solar zenith angle at mid of interval in degree',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `sun` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_7` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `sd_so` float NOT NULL COMMENT 'Hourly sunshine duration in minutes',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `wind` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `station_id` smallint unsigned NOT NULL,
    `measured_at` datetime NOT NULL,
    `qn_3` tinyint unsigned NOT NULL COMMENT 'Quality level (1-10; 10 = best)',
    `f` float NOT NULL COMMENT 'Mean wind speed in m/s',
    `d` smallint NOT NULL COMMENT 'Mean wind direction in degree',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

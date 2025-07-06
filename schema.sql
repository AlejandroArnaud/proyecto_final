/*
-------------------------------------------------------------------
-- Script DDL para la implementación del esquema OULAD en MySQL --
-------------------------------------------------------------------
-- Modelo de datos basado en el diagrama oficial del dataset y
-- enriquecido con campos de dominio calculados.
-------------------------------------------------------------------
*/

SET FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS `studentVle`;
DROP TABLE IF EXISTS `studentAssessment`;
DROP TABLE IF EXISTS `studentRegistration`;
DROP TABLE IF EXISTS `vle`;
DROP TABLE IF EXISTS `assessments`;
DROP TABLE IF EXISTS `studentInfo`;
DROP TABLE IF EXISTS `courses`;

-- Tabla: courses
CREATE TABLE `courses` (
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `module_presentation_length` INT,
  PRIMARY KEY (`code_module`, `code_presentation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: studentInfo
CREATE TABLE `studentInfo` (
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `id_student` INT NOT NULL,
  `gender` VARCHAR(3) NOT NULL,
  `region` VARCHAR(45) NOT NULL,
  `highest_education` VARCHAR(45) NOT NULL,
  `imd_band` VARCHAR(16) NULL,
  `age_band` VARCHAR(16) NOT NULL,
  `num_of_prev_attempts` INT NOT NULL,
  `studied_credits` INT NOT NULL,
  `disability` VARCHAR(3) NOT NULL,
  `final_result` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_student`, `code_module`, `code_presentation`),
  CONSTRAINT `fk_studentinfo_courses` FOREIGN KEY (`code_module`, `code_presentation`) REFERENCES `courses` (`code_module`, `code_presentation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: studentRegistration
CREATE TABLE `studentRegistration` (
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `id_student` INT NOT NULL,
  `date_registration` INT NULL,
  `date_unregistration` INT NULL,
  PRIMARY KEY (`id_student`, `code_module`, `code_presentation`),
  CONSTRAINT `fk_registration_studentinfo` FOREIGN KEY (`id_student`, `code_module`, `code_presentation`) REFERENCES `studentInfo` (`id_student`, `code_module`, `code_presentation`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: assessments
CREATE TABLE `assessments` (
  `id_assessment` INT NOT NULL,
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `assessment_type` VARCHAR(45) NOT NULL,
  `date` INT NULL,
  `weight` FLOAT NOT NULL,
  PRIMARY KEY (`id_assessment`),
  CONSTRAINT `fk_assessments_courses` FOREIGN KEY (`code_module`, `code_presentation`) REFERENCES `courses` (`code_module`, `code_presentation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: vle
CREATE TABLE `vle` (
  `id_site` INT NOT NULL,
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `activity_type` VARCHAR(45) NOT NULL,
  `week_from` INT NULL,
  `week_to` INT NULL,
  PRIMARY KEY (`id_site`),
  CONSTRAINT `fk_vle_courses` FOREIGN KEY (`code_module`, `code_presentation`) REFERENCES `courses` (`code_module`, `code_presentation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: studentAssessment
CREATE TABLE `studentAssessment` (
  `id_assessment` INT NOT NULL,
  `id_student` INT NOT NULL,
  `date_submitted` INT NOT NULL,
  `is_banked` TINYINT NOT NULL,
  `score` FLOAT NULL,
  `assessment_result` VARCHAR(10) NULL, -- Campo de dominio calculado
  PRIMARY KEY (`id_assessment`, `id_student`),
  CONSTRAINT `fk_studentassessment_assessment` FOREIGN KEY (`id_assessment`) REFERENCES `assessments` (`id_assessment`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla: studentVle
CREATE TABLE `studentVle` (
  `id_interaction` BIGINT NOT NULL AUTO_INCREMENT,
  `code_module` VARCHAR(45) NOT NULL,
  `code_presentation` VARCHAR(45) NOT NULL,
  `id_student` INT NOT NULL,
  `id_site` INT NOT NULL,
  `date` INT NOT NULL,
  `sum_click` INT NOT NULL,
  PRIMARY KEY (`id_interaction`),
  CONSTRAINT `fk_studentvle_registration` FOREIGN KEY (`id_student`, `code_module`, `code_presentation`) REFERENCES `studentRegistration` (`id_student`, `code_module`, `code_presentation`),
  CONSTRAINT `fk_studentvle_vle` FOREIGN KEY (`id_site`) REFERENCES `vle` (`id_site`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS=1;
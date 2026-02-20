variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type    = string
  default = "hoops-edge"
}

variable "checkpoint_table_name" {
  type    = string
  default = "cbbd_checkpoints"
}

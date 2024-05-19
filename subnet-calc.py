import ipaddress

def calculate_subnets(vpc_cidr, num_subnets):
    # Convert the VPC CIDR to an IPv4Network object
    vpc_network = ipaddress.ip_network(vpc_cidr)
    print(str(vpc_network))

    # Calculate the subnet mask length needed based on the number of subnets
    subnet_mask_length = vpc_network.prefixlen + (num_subnets).bit_length() - 1
    print(str(subnet_mask_length))
    # Generate the subnets
    subnets = list(vpc_network.subnets(new_prefix=subnet_mask_length))

    print(f"Subnet ranges for VPC CIDR {vpc_cidr} and {num_subnets} subnets:")
    for i, subnet in enumerate(subnets):
        print(f"Subnet {i+1}: {subnet}")

def main():
    vpc_cidr = input("Enter the VPC CIDR:- ")
    num_subnets = int(input("Enter the number of subnets you want to create: "))
    calculate_subnets(vpc_cidr, num_subnets)

if __name__ == "__main__":
    main()

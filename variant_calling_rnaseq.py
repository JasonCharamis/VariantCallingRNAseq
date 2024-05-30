
from prefect import task, flow
from prefect_dask.task_runners import DaskTaskRunner

# Parse command-line arguments
@task
def parse_arguments():

    # Import dependencies which are used in a single function locally
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description='Variant Calling from RNAseq data using samtools mpileup and VarScan2.'
    )

    parser.add_argument(
        '-c',
        '--config_file',
        type = str,
        required = True,
        help = 'Input yaml file with sample, genome names and various other configurations.'
    )

    parser.add_argument(
        '-ey',
        '--env_yaml',
        type = str,
        required = False,
        help = 'Input yaml file with pipeline dependencies.'
    )
    
    parser.add_argument(
        '-mw',
        '--maximum_workers',
        type = int,
        required = False,
        help='Maximum number of workers for adaptive scaling of Dask task runner. Default: 6'
    )
    
    try:
        args = parser.parse_args()
        print("Arguments parsed successfully.")
        return args

    except SystemExit as e:
        parser.print_help()               
        raise SystemExit("Argument parsing error", exc_info=True)

    
@task
def _load_yaml_file(yaml_file) -> dict:

    import yaml

    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config


@task
def install_dependencies(env_yaml: str) -> None:

    import subprocess
    subprocess.run(["mamba", "env", "update", "--file", env_yaml], check = True)
    config = _load_yaml_file(env_yaml)
    env_name = config['name']
    subprocess.run(["mamba", "env", "activate", env_name], check=True, shell=True)
    
    
@task
def mpileup(samples, genome) -> None:

    import subprocess
    
    bams = ' '.join([f'{sample}.*bam' for sample in samples])
    
    if bams:
        if genome:
            cmd = f"samtools mpileup -f {genome} -o all_samples.mpileup {bams}"
            
            try:               
                subprocess.run(cmd, shell=True)
                
            except Exception as e:
                raise Exception ( e )
        else:
            print (f"Provided {genome} index was not found.")
    else:
        print (f"Provided {bams} were not found.")

        
@task
def varscan(
    mpileup_file = 'all_samples.mpileup',
    pvalue = 0.01,
    output_format = 1,
    vcf_file = 'all_samples.vcf'
) -> None:

    import subprocess
    
    if mpileup:
        cmd = f"varscan mpileup2snp {mpileup} --p-value {pvalue} --output-vcf {output_format} --output {vcf_file} """    

        try:
            subprocess.run(cmd, shell=True)
        except Exception as e:
            raise Exception ( e )            
    else:
         print (f"Mpileup file {mpileup} was not found.")

         
@flow()
def main_flow() -> None:

    # Parse arguments                    
    args = parse_arguments()

    # Install dependencies using mamba
    install_dependencies(env_yaml = args.env_yaml if args.env_yaml else 'envs/envs.yaml')

    # Load config file
    yaml_file = args.config_file if args.config_file else 'config/config.yaml'
    config = _load_yaml_file(yaml_file = yaml_file)

    # Run samtools mpileup
    mpileup(samples = [sample.strip('\n') for sample in open(config['samples'], 'r')], genome = config['genome'])
    
    # Run VarScan2 to identify statistically significant SNPs
    varscan(
        mpileup = 'all_samples.mpileup',           
        pvalue = config['pvalue'] if isinstance(config['pvalue'], float) else 0.01,
        output_format = config['output_format'] if isinstance(config['output_format'], int) else 1,
        vcf_file = config['vcf_file'] if isinstance(config['vcf_file'], str) else 'all_samples.vcf'
    )
        
                

if __name__ == "__main__":
#    main_flow.visualize()
    main_flow()
